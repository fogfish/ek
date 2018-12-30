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
   quorum    = undefined :: integer(),     %% number of online nodes
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

start_link(Id, Opts) 
 when is_atom(Id) ->
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
         quorum    = proplists:get_value(quorum, Opts, #{}),
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
   peers_quorum(State#state{peers = bst:insert(Peer, Ref, Peers)}).


%%
%%
peerdown(Peer, #state{peers = Peers} = State) ->
   case bst:lookup(Peer, Peers) of
      undefined ->
         State;
      Ref ->
         vnode_quorum(
            peerdown_handoff(Peer,
               peerdown_remove(Peer, Ref, State)
            )
         )
   end.

peerdown_remove(Peer, Ref, #state{id = Id, peers = Peers} = State) ->
   ?DEBUG("[ek]: pg ~s peerdown ~s~n", [Id, Peer]),
   erlang:demonitor(Ref, [flush]),
   peers_quorum(State#state{peers = bst:remove(Peer, Peers)}).

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
peers_quorum(#state{quorum = #{ peers := Q }, peers = Peers} = State) ->
   %% Note: itself is not counted as peer
   case length(bst:keys(Peers)) + 1 of
      N when N < Q ->
         send_to_local({quorum, peers, false}, State);
      _ ->
         send_to_local({quorum, peers,  true}, State)
   end,
   State;

peers_quorum(#state{} = State) ->
   State.

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
   vnode_quorum(State#state{watchdogs = Refs1, processes = Pids1}).

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
   vnode_quorum(State1).

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
   vnode_quorum(State1).

%%
%%
vnode_quorum(#state{quorum = #{ vnode := Q }, processes = Pids} = State) ->
   case length(crdts_orset:value(Pids)) of
      N when N < Q ->
         send_to_local({quorum, vnode, false}, State);
      _ ->
         send_to_local({quorum, vnode,  true}, State)
   end,
   State;

vnode_quorum(#state{} = State) ->
   State.



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
gossip_reconcile(Remote, #state{processes = Local} = State0) ->
   State1 = State0#state{processes = crdts_orset:join(Remote, Local)},
   gossip_reconcile_sub(diff(Local, Remote), State1),
   gossip_reconcile_add(diff(Remote, Local), State1),
   State1.


gossip_reconcile_sub([], State) ->
   State;
gossip_reconcile_sub(Pids, #state{id = Id} = State) ->
   ?DEBUG("[ek]: pg ~s {-} ~p~n", [Id, Pids]),
   lists:foreach(
      fun({Vnode, Pid}) ->
         send_to_local({handoff, Vnode, Pid}, State)
      end,
      Pids
   ),
   vnode_quorum(State).

gossip_reconcile_add([], State) ->
   State;
gossip_reconcile_add(Pids, #state{id = Id} = State) ->
   ?DEBUG("[ek]: pg ~s {+} ~p~n", [Id, Pids]),
   lists:foreach(
      fun({Vnode, Pid}) ->
         send_to_local({join, Vnode, Pid}, State)
      end,
      Pids
   ),
   vnode_quorum(State).

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
   %% Note: sync communication with attached process is required to ensure
   %%       state consistency (e.g. routing tables) before local peers updated
   %%       with information about new node 
   [catch gen_server:call(Pid, Msg, 60000) || Pid <- EventBus],
   [Pid ! Msg || 
      {_VNode, Pid} <- crdts_orset:value(Pids),
      erlang:node(Pid) =:= erlang:node()
   ].
