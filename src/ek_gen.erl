%% @doc
%%   generic topology management process, observes environment and manages actors 
-module(ek_gen).
-behaviour(pipe).

-include("ek.hrl").
-compile({parse_transform, category}).

-export([
   start_link/3,
   init/1,
   free/2,
   handle/3
]).

%%
%%
-record(state, {
   id        = undefined :: atom(),        %% globally unique identity of topology
   mod       = undefined :: atom(),        %% topology controller module
   n         = undefined :: integer(),     %% number of shards 
   peers     = undefined :: _,             %% remote peers
   processes = undefined :: crdts:orset(), %% global process set
   watchdogs = undefined :: _,             %% local process watchdogs
   topology  = undefined :: _              %% custom topology state
}).

%%%------------------------------------------------------------------
%%%
%%% factory
%%%
%%%------------------------------------------------------------------   

start_link(Id, Mod, Opts) ->
   pipe:start_link({local, Id}, ?MODULE, [Id, Mod, Opts], []).

%%
init([Id, Mod, Opts]) ->
   [either || 
      net_kernel:monitor_nodes(true),
      State <- empty(Id, Mod, Opts),
      cast_to_peers({peerup, erlang:node()}, State),
      gossip_schedule(
         proplists:get_value(gossip,   Opts, ?CONFIG_GOSSIP_TIMEOUT),
         proplists:get_value(exchange, Opts, ?CONFIG_GOSSIP_EXCHANGE)
      ),
      cats:unit(handle, State)
   ].

empty(Id, Mod, Opts) ->
   {ok, #state{
      id        = Id,
      mod       = Mod,
      n         = proplists:get_value(n, Opts, 3),
      peers     = bst:new(),
      processes = crdts_orset:new(),
      watchdogs = bst:new(),
      topology  = Mod:init(Id, Opts)
   }}.

%%
free(_Reason, _State) ->
   ok.

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

handle(peers, Pipe, #state{peers = Peers} = State) ->
   pipe:ack(Pipe, bst:keys(Peers)),
   {next_state, handle, State};

%%
%% actor management
%%
handle({join, VNode, Pid}, Pipe, State) ->
   State1 = join(VNode, Pid, State),
   pipe:ack(Pipe, ok),
   {next_state, handle, State1};

handle({leave, Pid}, Pipe, State0) ->
   State1 = leave(Pid, State0),
   pipe:ack(Pipe, ok),
   {next_state, handle, State1};

handle({'DOWN', _Ref, process, Pid, _Reason}, _, State) ->
   {next_state, handle, leave(Pid, State)};

handle({gossip, After, With}, _, #state{} = State) ->
   gossip_exchange(With, State),
   gossip_schedule(After, With),
   {next_state, handle, State};

handle({reconcile, Remote}, _, #state{} = State) ->
   {next_state, handle, gossip_reconcile(Remote, State)};

handle(members, Pipe, #state{processes = Pids} = State) ->
   pipe:ack(Pipe, crdts_orset:value(Pids)),
   {next_state, handle, State};

handle(size, Pipe, #state{processes = Pids} = State) ->
   pipe:ack(Pipe, length(crdts_orset:value(Pids))),
   {next_state, handle, State};

handle(address, Pipe, #state{processes = Pids, mod = Mod, topology = Topo} = State) ->
   pipe:ack(Pipe, Mod:address(Pids, Topo)),
   {next_state, handle, State};

handle({whois, Key}, Pipe, #state{processes = Pids, mod = Mod, topology = Topo} = State) ->
   pipe:ack(Pipe, Mod:whois(Key, Pids, Topo)),
   {next_state, handle, State};

handle({predecessors, Key}, Pipe, #state{processes = Pids, mod = Mod, n = N, topology = Topo} = State) ->
   pipe:ack(Pipe, Mod:predecessors(N, Key, Pids, Topo)),
   {next_state, handle, State};

handle({successors, Key}, Pipe, #state{processes = Pids, mod = Mod, n = N, topology = Topo} = State) ->
   pipe:ack(Pipe, Mod:successors(N, Key, Pids, Topo)),
   {next_state, handle, State}.


%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   

%%
%%
nodeup(Node, #state{id = Id} = State) ->
   ?DEBUG("ek: topology ~s nodeup ~s~n", [Id, Node]),
   erlang:send({Id, Node}, {peerup, erlang:node()}),
   State.


%%
%%
peerup(Node, #state{peers = Peers} = State) ->
   case bst:lookup(Node, Peers) of
      undefined ->
         peerup_append_peer(Node, State);
      _ ->
         State
   end.

peerup_append_peer(Node, #state{id = Id, peers = Peers} = State) ->
   ?DEBUG("ek: topology ~s peerup ~s~n", [Id, Node]),
   Ref = erlang:monitor(process, {Id, Node}),
   send_to_peers(Node, {peerup, erlang:node()}, State),
   State#state{peers = bst:insert(Node, Ref, Peers)}.


%%
%%
peerdown(Node, #state{peers = Peers} = State) ->
   case bst:lookup(Node, Peers) of
      undefined ->
         State;
      Ref ->
         peerdown_forget_processes(Node,
            peerdown_remove_peer(Node, Ref, State)
         )
   end.

peerdown_remove_peer(Node, Ref, #state{id = Id, peers = Peers} = State) ->
   ?DEBUG("ek: topology ~s peerdown ~s~n", [Id, Node]),
   erlang:demonitor(Ref, [flush]),
   State#state{peers = bst:remove(Node, Peers)}.

peerdown_forget_processes(Node, #state{processes = Pids} = State) ->
   % death of node requires unconditional removal of all its processes
   % dead nodes do not run reconciliation process
   State#state{
      processes = crdts_orset:filter(
         fun({VNode, Pid}) ->
            case erlang:node(Pid) of
               Node ->
                  send_to_local({leave, VNode, Pid}, State),
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
join(VNode, Pid, #state{processes = Pids} = State) ->
   case lists:keyfind(Pid, 2, crdts_orset:value(Pids)) of
      false ->
         join_vnode(VNode, Pid, State);
      _ ->
         State
   end.

join_vnode(VNode, Pid, #state{processes = Pids0, watchdogs = Refs0} = State0) ->
   Refs1  = bst:insert(Pid, erlang:monitor(process, Pid), Refs0),
   Pids1  = crdts_orset:insert({VNode, Pid}, Pids0),
   State1 = State0#state{watchdogs = Refs1, processes = Pids1},
   [Pid ! {join, VNodeEx, PidEx} || {VNodeEx, PidEx} <- crdts_orset:value(Pids0)],
   send_to_local({join, VNode, Pid}, State0),
   State1.


%%
%%
leave(Pid, #state{processes = Pids} = State) ->
   case lists:keyfind(Pid, 2, crdts_orset:value(Pids)) of
      false ->
         State;
      {VNode, _} ->
         leave_vnode(VNode, Pid, State)
   end.

leave_vnode(VNode, Pid, #state{processes = Pids0, watchdogs = Refs0} = State0) ->
   Pids1  = crdts_orset:remove({VNode, Pid}, Pids0),
   Ref    = bst:lookup(Pid, Refs0),
    _     = erlang:demonitor(Ref, [flush]),
   Refs1  = bst:remove(Pid, Refs0),
   State1 = State0#state{watchdogs = Refs1, processes = Pids1},
   send_to_local({leave, VNode, Pid}, State1),
   State1.

%%
%%
gossip_schedule(Timeout, With) ->
   erlang:send_after(Timeout, self(), {gossip, Timeout, With}).

%%
%%
gossip_exchange(With, #state{id = Id, processes = Pids} = State) ->
   [erlang:send({Id, Peer}, {reconcile, Pids}) || Peer <- gossip_with(With, State)].

gossip_with(N, #state{peers = Peers} = State) ->
   gossip_with_peers(N, bst:keys(Peers)).

gossip_with_peers(_, []) ->
   [];
gossip_with_peers(N, Peers) ->
   lists:usort(
      lists:map(
         fun(_) ->
            lists:nth(random:uniform(length(Peers)), Peers)
         end,
         lists:seq(1, N)
      )
   ).

%%
%%
gossip_reconcile(Remote, #state{id = Id, processes = Local} = State) ->
   gossip_reconcile_sub(diff(Local, Remote), Remote, State),
   gossip_reconcile_add(diff(Remote, Local), Remote, State),
   State#state{processes = crdts_orset:join(Remote, Local)}.


gossip_reconcile_sub([], _, _State) ->
   ok;
gossip_reconcile_sub(Pids, _, #state{id = Id, processes = Local} = State) ->
   ?DEBUG("ek: topology ~s [-] ~p~n", [Id, Pids]),
   lists:foreach(
      fun(Pid) ->
         Addr = orddict:fetch(Pid, crdts_orset:value(Local)),
         send_to_local({leave, Addr, Pid}, State)
      end,
      Pids
   ).

gossip_reconcile_add([], _, _State) ->
   ok;
gossip_reconcile_add(Pids, Remote, #state{id = Id} = State) ->
   ?DEBUG("ek: topology ~s [+] ~p~n", [Id, Pids]),
   lists:foreach(
      fun(Pid) ->
         Addr = orddict:fetch(Pid, crdts_orset:value(Remote)),
         send_to_local({join, Addr, Pid}, State)
      end,
      Pids
   ).

%%
%% Returns only the elements of SetA that are not also elements of SetB.
diff(OrSetA, OrSetB) ->
   A = [Pid || 
      {_VNode, Pid} <- crdts_orset:value(OrSetA), 
      erlang:node(Pid) =/= erlang:node()],

   B = [Pid || 
      {_VNode, Pid} <- crdts_orset:value(OrSetB), 
      erlang:node(Pid) =/= erlang:node()],

   gb_sets:to_list(
      gb_sets:difference(
         gb_sets:from_list(A),
         gb_sets:from_list(B)
      )
   ).

%%
%%
cast_to_peers(Msg, #state{id = Id}) ->
   [erlang:send({Id, Node}, Msg) || Node <- erlang:nodes()].

%%
%%
send_to_peers(Node, Msg, #state{id = Id}) ->
   erlang:send({Id, Node}, Msg).

%%
%%
send_to_local(Msg, #state{processes = Pids}) ->
   [Pid ! Msg || 
      {_VNode, Pid} <- crdts_orset:value(Pids),
      erlang:node(Pid) =:= erlang:node()
   ].



