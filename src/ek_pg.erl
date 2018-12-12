%% @doc
%%   generic topology management process, observes environment and manages actors 
-module(ek_pg).
-behaviour(pipe).

-include("ek.hrl").

-export([
   start_link/2,
   init/1,
   ioctl/2,
   free/2,
   handle/3
]).

%%
%%
-record(state, {
   id        = undefined :: atom(),        %% globally unique identity of topology
   eventbus  = undefined :: [atom()],      %% event bus       
   peers     = undefined :: _,             %% remote peers
   processes = undefined :: crdts:orset(), %% global process set
   watchdogs = undefined :: _              %% local process watchdogs
}).

%%%------------------------------------------------------------------
%%%
%%% factory
%%%
%%%------------------------------------------------------------------   

start_link(Id, Opts) ->
   pipe:start_link({local, Id}, ?MODULE, [Id, Opts], []).

%%
init([Id, Opts]) ->
   ok = net_kernel:monitor_nodes(true),
   [erlang:send({Id, Peer}, {peerup, erlang:node()}) || Peer <- erlang:nodes()],
   gossip_schedule(
      proplists:get_value(gossip,   Opts, ?CONFIG_GOSSIP_TIMEOUT),
      proplists:get_value(exchange, Opts, ?CONFIG_GOSSIP_EXCHANGE)
   ),
   {ok, handle,
      #state{
         id        = Id,
         eventbus  = [],
         peers     = bst:new(),
         processes = crdts_orset:new(),
         watchdogs = bst:new()
      }
   }.

%%
free(_Reason, _State) ->
   ok.

%%
ioctl({attach, Pid}, #state{eventbus = Pids} = State) ->
   State#state{eventbus = [Pid | Pids]};

ioctl({detach, Pid}, #state{eventbus = Pids} = State) ->
   State#state{eventbus = [X || X <- Pids, X /= Pid]}.

%%%------------------------------------------------------------------
%%%
%%% state machine
%%%
%%%------------------------------------------------------------------   

%%
%% peer managements
%%
handle({peerup, Node}, _, #state{} = State) ->
   {next_state, handle, peerup(Node, State)};

handle({nodeup, Node}, _, #state{} = State) ->
   {next_state, handle, nodeup(Node, State)};

handle({'DOWN', _Ref, process, {Id, Node}, _Reason}, _, #state{id = Id} = State) ->
   {next_state, handle, peerdown(Node, State)};

handle({nodedown, Node}, _, State) ->
   {next_state, handle, peerdown(Node, State)};

handle(peers, _, #state{peers = Peers} = State) ->
   {reply, bst:keys(Peers), State};


%%
%% virtual node management
%%
handle({join, Vnode, Pid}, _, State) ->
   {reply, ok, join(Vnode, Pid, State)};

handle({leave, Vnode}, _, State) ->
   {reply, ok, leave(Vnode, State)};

handle({'DOWN', _Ref, process, Pid, _Reason}, _, State) ->
   {next_state, handle, handoff(Pid, State)};

handle({gossip, After, With}, _, #state{} = State) ->
   gossip_exchange(With, State),
   gossip_schedule(After, With),
   {next_state, handle, State};

handle({reconcile, Remote}, _, #state{} = State) ->
   {next_state, handle, gossip_reconcile(Remote, State)};

handle(members, _, #state{processes = Pids} = State) ->
   {reply, crdts_orset:value(Pids), State}.


%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   

%%
%%
nodeup(Node, #state{id = Id} = State) ->
   ?DEBUG("[ek]: pg ~s nodeup ~s~n", [Id, Node]),
   erlang:send({Id, Node}, {peerup, erlang:node()}),
   State.


%%
%%
peerup(Peer, #state{peers = Peers} = State) ->
   case bst:lookup(Peer, Peers) of
      undefined ->
         peerup_append(Peer, State);
      _ ->
         State
   end.

peerup_append(Peer, #state{id = Id, peers = Peers} = State) ->
   ?DEBUG("[ek]: pg ~s peerup ~s~n", [Id, Peer]),
   Ref = erlang:monitor(process, {Id, Peer}),
   erlang:send({Id, Peer}, {peerup, erlang:node()}),
   State#state{peers = bst:insert(Peer, Ref, Peers)}.


%%
%%
peerdown(Peer, #state{peers = Peers} = State) ->
   case bst:lookup(Peer, Peers) of
      undefined ->
         State;
      Ref ->
         peerdown_handoff(Peer,
            peerdown_remove(Peer, Ref, State)
         )
   end.

peerdown_remove(Peer, Ref, #state{id = Id, peers = Peers} = State) ->
   ?DEBUG("[ek]: pg ~s peerdown ~s~n", [Id, Peer]),
   erlang:demonitor(Ref, [flush]),
   State#state{peers = bst:remove(Peer, Peers)}.

peerdown_handoff(Peer, #state{processes = Pids} = State) ->
   % death of node requires unconditional removal of all its processes
   % dead nodes do not run reconciliation process
   State#state{
      processes = crdts_orset:filter(
         fun({Vnode, Pid}) ->
            case erlang:node(Pid) of
               Peer ->
                  send_to_local({handoff, Vnode, Pid}, State),
                  false;
               _ ->
                  true
            end
         end,
         Pids
      )
   }.

%%
%%
join(Vnode, Pid, #state{processes = Pids} = State) ->
   case lists:keyfind(Vnode, 1, crdts_orset:value(Pids)) of
      false ->
         join_vnode(Vnode, Pid, State);
      _ ->
         State
   end.

join_vnode(Vnode, Pid, #state{processes = Pids0, watchdogs = Refs0} = State) ->
   Refs1  = bst:insert(Pid, erlang:monitor(process, Pid), Refs0),
   Pids1  = crdts_orset:insert({Vnode, Pid}, Pids0),
   [Pid ! {join, VnodeEx, PidEx} || {VnodeEx, PidEx} <- crdts_orset:value(Pids0)],
   send_to_local({join, Vnode, Pid}, State),
   State#state{watchdogs = Refs1, processes = Pids1}.

%%
%%
handoff(Pid, #state{processes = Pids} = State) ->
   lists:foldl(
      fun({Vnode, _}, Acc) -> handoff_vnode(Vnode, Pid, Acc) end,
      State,
      lists:filter(
         fun({_, X}) -> X =:= Pid end,
         crdts_orset:value(Pids)
      )
   ).

handoff_vnode(Vnode, Pid, #state{processes = Pids0, watchdogs = Refs0} = State0) ->
   Pids1  = crdts_orset:remove({Vnode, Pid}, Pids0),
   Ref    = bst:lookup(Pid, Refs0),
    _     = erlang:demonitor(Ref, [flush]),
   Refs1  = bst:remove(Pid, Refs0),
   State1 = State0#state{watchdogs = Refs1, processes = Pids1},
   send_to_local({handoff, Vnode, Pid}, State1),
   State1.

%%
%%
leave(Vnode, #state{processes = Pids} = State) ->
   case lists:keyfind(Vnode, 1, crdts_orset:value(Pids)) of
      false ->
         State;
      {_, Pid} ->
         leave_vnode(Vnode, Pid, State)
   end.

leave_vnode(Vnode, Pid, #state{processes = Pids0, watchdogs = Refs0} = State0) ->
   Pids1  = crdts_orset:remove({Vnode, Pid}, Pids0),
   Ref    = bst:lookup(Pid, Refs0),
    _     = erlang:demonitor(Ref, [flush]),
   Refs1  = bst:remove(Pid, Refs0),
   State1 = State0#state{watchdogs = Refs1, processes = Pids1},
   send_to_local({leave, Vnode, Pid}, State1),
   State1.


%%
%%
gossip_schedule(Timeout, With) ->
   erlang:send_after(Timeout, self(), {gossip, Timeout, With}).

%%
%%
gossip_exchange(With, #state{id = Id, processes = Pids} = State) ->
   [erlang:send({Id, Peer}, {reconcile, Pids}) || Peer <- gossip_with(With, State)].

gossip_with(N, #state{peers = Peers}) ->
   gossip_with_peers(N, bst:keys(Peers)).

gossip_with_peers(_, []) ->
   [];
gossip_with_peers(N, Peers) ->
   lists:usort(
      lists:map(
         fun(_) ->
            lists:nth(rand:uniform(length(Peers)), Peers)
         end,
         lists:seq(1, N)
      )
   ).

%%
%%
gossip_reconcile(Remote, #state{processes = Local} = State) ->
   gossip_reconcile_sub(diff(Local, Remote), State),
   gossip_reconcile_add(diff(Remote, Local), State),
   State#state{processes = crdts_orset:join(Remote, Local)}.


gossip_reconcile_sub([], _State) ->
   ok;
gossip_reconcile_sub(Pids, #state{id = Id} = State) ->
   ?DEBUG("[ek]: pg ~s {-} ~p~n", [Id, Pids]),
   lists:foreach(
      fun({Vnode, Pid}) ->
         send_to_local({handoff, Vnode, Pid}, State)
      end,
      Pids
   ).

gossip_reconcile_add([], _State) ->
   ok;
gossip_reconcile_add(Pids, #state{id = Id} = State) ->
   ?DEBUG("[ek]: pg ~s {+} ~p~n", [Id, Pids]),
   lists:foreach(
      fun({Vnode, Pid}) ->
         send_to_local({join, Vnode, Pid}, State)
      end,
      Pids
   ).

%%
%% Returns only the elements of SetA that are not also elements of SetB.
%% Note: we exclude all local process from reconciliation because the peer 
%%       is a master of all local processes
diff(OrSetA, OrSetB) ->
   A = [E || 
      {_, Pid} = E <- crdts_orset:value(OrSetA), 
      erlang:node(Pid) =/= erlang:node()],

   B = [E || 
      {_, Pid} = E <- crdts_orset:value(OrSetB), 
      erlang:node(Pid) =/= erlang:node()],

   gb_sets:to_list(
      gb_sets:difference(
         gb_sets:from_list(A),
         gb_sets:from_list(B)
      )
   ).

%%
%%
send_to_local(Msg, #state{processes = Pids, eventbus = EventBus}) ->
   [catch gen_server:call(Pid, Msg, 60000) || Pid <- EventBus],
   [Pid ! Msg || 
      {_VNode, Pid} <- crdts_orset:value(Pids),
      erlang:node(Pid) =:= erlang:node()
   ].
