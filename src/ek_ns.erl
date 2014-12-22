%%
%%   Copyright 2012 Dmitry Kolesnikov, All Rights Reserved
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
%% @description
%%   the distributed ring topology management process. The topology uses
%%   consistent hashing to form cluster of processes.
%%   
%%   This topology implements different logic on outage handling then
%%   process group (pg). It assumes transient failures due to maintenance tasks,
%%   node crash or network failure. The member outage is rarely seen as permanent 
%%   departure, therefore expensive re-balancing and partition recovery is not
%%   performed. The name space requires explicit mechanism to alter (join/leave)
%%   membership state. It uses gossip push protocol to exchange ring state.
%%
%%   single name space process ensure 40K routing per second, any other requires
%%   parallel
-module(ek_ns).
-behaviour(gen_server).
-include("ek.hrl").

-export([
   start_link/2
  ,init/1
  ,terminate/2
  ,handle_call/3
  ,handle_cast/2
  ,handle_info/2
  ,code_change/3

  ,diff/3
]).

%%
%% internal state
-record(srv, {
   name     = undefined :: atom()       %% identity of name-space
  ,mod      = undefined :: chord | ring %% consistent hash type
  ,ring     = undefined :: datum:ring() %% name-space ring
  ,peer     = undefined :: datum:tree() %% list of remote peers (nodes running ns)
  ,vclock   = undefined :: any()        %% vector clock of the ring
  ,gossip   = undefined :: integer()    %% gossip timeout
  ,quorum   = undefined :: integer()    %% 
  ,exchange = undefined :: integer()    %% messages to forward per gossip exchange 
}).


%%%------------------------------------------------------------------
%%%
%%% factory
%%%
%%%------------------------------------------------------------------   

start_link(Name, Opts) ->
   gen_server:start_link({local, Name}, ?MODULE, [Name, Opts], []).

init([Name, Opts]) ->
   ?DEBUG("ek [ns]: new ~s~n", [Name]),
   %% notify all known cluster nodes that topology peer is up
   [erlang:send({Name, X}, {peerup, erlang:node()}) || X <- erlang:nodes()], 
   _ = net_kernel:monitor_nodes(true),
   _ = erlang:send(self(), gossip),
   _ = random:seed(os:timestamp()),
   {_, Mod} = lists:keyfind(type, 1, Opts),
   {ok, 
      #srv{
         name     = Name
        ,mod      = Mod
        ,ring     = Mod:new(Opts)
        ,peer     = bst:new()
        ,vclock   = ek_vclock:new()
        ,gossip   = proplists:get_value(gossip,   Opts, ?CONFIG_GOSSIP_TIMEOUT)
        ,quorum   = proplists:get_value(quorum,   Opts)
        ,exchange = proplists:get_value(exchange, Opts, ?CONFIG_GOSSIP_EXCHANGE)
      }
   }.

terminate(_, _S) ->
   ok.

%%%------------------------------------------------------------------
%%%
%%% gen_server
%%%
%%%------------------------------------------------------------------   

%%
%%
handle_call({join, Id, Pid}, _, State) ->
   {reply, ok, join_process(Id, {erlang:node(), Pid}, State)};

handle_call({leave, Id}, _, State) ->
   {reply, ok, leave_process(Id, State)};

handle_call({address, Key}, _Tx, #srv{mod=Mod}=State) ->
   % list vnode addresses
   Result = [Addr || {Addr, _Key} <- Mod:lookup(Key, State#srv.ring)],
   {reply, Result, State};

handle_call({predecessors, Key}, _Tx, #srv{mod=Mod}=State) ->
   {reply, whereis(Key, fun Mod:predecessors/3, Mod, State#srv.ring), State};

handle_call({successors, Key}, _Tx, #srv{mod=Mod}=State) ->
   {reply, whereis(Key, fun Mod:successors/3, Mod, State#srv.ring), State};

handle_call({ioctl, peers}, _Tx, State) ->
   % list all remote peers (nodes running name space leader)
   Result = [Peer || {Peer, _} <- bst:list(State#srv.peer)],
   {reply, Result, State};

handle_call({ioctl, members}, _Tx, #srv{mod=Mod}=State) ->
   % list all group members (processes members of ring)
   Result = [{Key, Pid} || {Key, {_, _, Pid}} <- Mod:members(State#srv.ring)],
   {reply, Result, State};

handle_call({ioctl, quorum}, _Tx, #srv{quorum=undefined}=State) ->
   {reply, true, State};

handle_call({ioctl, {lookup, Key}}, _Tx, #srv{mod = Mod, ring = Ring}=State) ->
   {reply, Mod:lookup(Key, Ring), State};

handle_call({ioctl, quorum}, _Tx, #srv{quorum=N, mod=Mod}=State) ->
   case Mod:size(State#srv.ring) of
      X when X >= N ->
         {reply, true,  State};
      _ ->
         {reply, false, State}
   end;

handle_call(Msg, _Tx, State) ->
   error_logger:warning_msg("ek [ns]: ~s unexpected message ~p~n", [State#srv.name, Msg]),
   {noreply, State}.

%%
%%
handle_cast(Msg, State) ->
   error_logger:warning_msg("ek [ns]: ~s unexpected message ~p~n", [State#srv.name, Msg]),
   {noreply, State}.

%%
%%
handle_info({peerup, Node}, State) ->
   {noreply, join_peer(Node, State)};

handle_info({nodeup, Node}, State) ->
   erlang:send({State#srv.name, Node}, {peerup, erlang:node()}),
   {noreply, State};

handle_info({nodedown, Node}, State) ->
   {noreply, leave_peer(Node, State)};

handle_info({'DOWN', _, _, {_, Node}, _Reason}, State) ->
   {noreply, leave_peer(Node, State)};

handle_info({'DOWN', _, _, Pid, _Reason}, State) ->
   {noreply, failure_process(Pid, State)};

handle_info({join, Id,  Pid}, State) ->
   {noreply, join_process(Id, Pid, State)};

handle_info({leave, Id}, State) ->
   {noreply, leave_process(Id, State)};

handle_info(gossip, #srv{mod=Mod}=State) ->   
   erlang:send_after(State#srv.gossip, self(), gossip),
   Msg  = {reconcile, erlang:node(), State#srv.vclock, Mod:members(State#srv.ring)},
   lists:foreach(
      fun(Peer) ->
         erlang:send({State#srv.name, Peer}, Msg)
      end,
      random_peer(State#srv.exchange, State)
   ),
   {noreply, State};

handle_info({reconcile, _Peer, Vpeer, Vring}, #srv{mod=Mod, vclock=Va}=State) ->
   {VClock, Join, Leave} = reconcile(
      State#srv.vclock
     ,Mod:members(State#srv.ring)
     ,Vpeer
     ,Vring
   ),
   State1 = lists:foldl(
      fun(Id, Acc) ->
         {_, {Peer, _, Pid}} = lists:keyfind(Id, 1, Vring),
         join_process(Id, {Peer, Pid}, Acc)
      end,
      State#srv{vclock = VClock},
      Join
   ),
   State2 = lists:foldl(
      fun(Id, Acc) ->
         leave_process(Id, Acc)
      end,
      State1,
      Leave  
   ),
   {noreply, State2};
      
handle_info(Msg, State) ->
   error_logger:warning_msg("ek [ns]: ~s unexpected message ~p~n", [State#srv.name, Msg]),
   {noreply, State}.   

%%
%%
code_change(_OldVsn, S, _Extra) ->
   {ok, S}.

%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   

%%
%% 
join_peer(Peer, State) ->
   case bst:lookup(Peer, State#srv.peer) of
      undefined ->
         peerup(Peer, State);
      _Ref      ->
         State
   end.

peerup(Node, State) ->
   ?DEBUG("ek [ns]: ~s peerup ~s~n", [State#srv.name, Node]),
   Ref = erlang:monitor(process, {State#srv.name, Node}),
   _   = erlang:send({State#srv.name, Node}, {peerup, erlang:node()}),
   State#srv{
      peer = bst:insert(Node, Ref, State#srv.peer)
   }.

%%
%%
leave_peer(Node, State) ->
   case bst:lookup(Node, State#srv.peer) of
      %% peer is not known at group
      undefined ->
         State;
      Ref       ->
         peerdown(Node, Ref, State)
   end.

peerdown(Node, Ref, State) ->
   ?DEBUG("ek [ns]: ~s peerdown ~s~n", [State#srv.name, Node]),
   _   = erlang:demonitor(Ref, [flush]),
   State#srv{
      peer = bst:remove(Node, State#srv.peer)
   }.

%%
%% select N-random peers
random_peer(N, #srv{peer=Peer}) ->
   random_peer(N, bst:list(Peer));
random_peer(_, []) ->
   [];
random_peer(N, Peers) ->
   lists:usort(
      lists:map(
         fun(_) ->    
            erlang:element(1,
               lists:nth(
                  random:uniform(length(Peers))
                 ,Peers
               )
            )
         end,
         lists:seq(1, N)
      )
   ).

%%
%% join process to ring / re-join failed process
join_process(Id, {Peer, Pid}, #srv{mod=Mod}=State) ->
   try
      case Mod:get(Id, State#srv.ring) of
         {_, _, X} when X =/= Pid ->
            join_to_ring(Id, {Peer, Pid}, State);
         _ ->
            State
      end
   catch _:badarg ->
      join_to_ring(Id, {Peer, Pid}, State)
   end.

join_to_ring(Key, {Node, undefined}, #srv{mod=Mod}=State) ->
   ?DEBUG("ek [ns]: ~s join ~p ~p~n", [State#srv.name, Key, undefined]),
   State#srv{
      ring   = Mod:join(Key, {Node, undefined, undefined}, State#srv.ring)
     ,vclock = ek_vclock:inc(State#srv.vclock)
   };

join_to_ring(Key, {Peer, Pid}, #srv{mod=Mod}=State)
 when Peer =:= erlang:node() ->
   ?DEBUG("ek [ns]: ~s join ~p ~p~n", [State#srv.name, Key, Pid]),
   Ref = erlang:monitor(process, Pid),
   notify({join, Key, Pid}, Mod, State#srv.ring),
   [erlang:send(Pid, {join, K, P}) || 
      {K, {N, _, P}} <- Mod:members(State#srv.ring),
                        N =:= erlang:node(),
                        P =/= undefined
   ],
   State#srv{
      ring   = Mod:join(Key, {Peer, Ref, Pid}, State#srv.ring)
     ,vclock = ek_vclock:inc(State#srv.vclock)
   };

join_to_ring(Key, {Peer, Pid}, #srv{mod=Mod}=State) ->
   ?DEBUG("ek [ns]: ~s join ~p ~p~n", [State#srv.name, Key, Pid]),
   Ref = erlang:monitor(process, Pid),
   notify({join, Key, Pid}, Mod, State#srv.ring),
   State#srv{
      ring   = Mod:join(Key, {Peer, Ref, Pid}, State#srv.ring)
     ,vclock = ek_vclock:inc(State#srv.vclock)
   }.

%%
%% process is explicitly left from the ring 
leave_process(Key, #srv{mod=Mod}=State) ->
   try
      ?DEBUG("ek [ns]: ~s leave ~p~n", [State#srv.name, Key]),
      case Mod:get(Key, State#srv.ring) of
         {_, _, undefined} ->
            ok;
         {_, Ref, _} ->
            erlang:demonitor(Ref, [flush])
      end,
      notify({leave, Key}, Mod, State#srv.ring),
      State#srv{
         ring   = Mod:leave(Key, State#srv.ring)
        ,vclock = ek_vclock:inc(State#srv.vclock)
      }
   catch _:badarg ->
      State
   end.

%%
%% transient failure of ring member
%% the "key" is still allocated by ring member but 
%% writes hand-off to another partition 
failure_process(Pid, #srv{mod=Mod}=State) ->
   ?DEBUG("ek [ns]: ~s failed ~p~n", [State#srv.name, Pid]),
   List = Mod:members(State#srv.ring),
   case [X || X = {_, {_, _, P}} <- List, P =:= Pid] of
      [{Key, {Node, Ref, Pid}}] ->
         erlang:demonitor(Ref, [flush]),
         notify({handoff, Key}, Mod, State#srv.ring),
         State#srv{
            ring = Mod:join(Key, {Node, undefined, undefined}, State#srv.ring)
         };
      _ ->
         State
   end.


%%
%% notify local processes
notify(Msg, Mod, Ring) ->
   lists:foreach(
      fun({_, {Node, _, Pid}}) ->
         if
            Node =:= erlang:node(), Pid =/= undefined ->
               erlang:send(Pid, Msg);
            true ->
               ok
         end
      end,
      Mod:members(Ring)
   ).      



%%
%% return list of nodes for key
whereis(Key0, Fun, Mod, Ring) ->
   N = Mod:n(Ring),
   Nodes = [{Addr, Key, Pid} || 
      {Addr, Key} <- Fun(N * 2, Key0, Ring), 
      {_, _, Pid} <- [Mod:get(Key, Ring)]
   ],
   case length(Nodes) of
      L when L =< N ->
         [{primary, Addr, Key, Pid} || {Addr, Key, Pid} <- Nodes, Pid =/= undefined];
      _ ->
         {Primary, Handoff} = lists:split(N, Nodes),
         handoff(Primary, Handoff)
   end.

%%
%% build hand-off list
handoff([{_, _, undefined}|_]=Primary, [{_, _, undefined}|Handoff]) ->
   handoff(Primary, Handoff);

handoff([{_, _, undefined}|Primary], []) ->
   handoff(Primary, []);

handoff([{Addr, Key, undefined} | Primary], [{_, _, Pid} | Handoff]) ->
   [{handoff, Addr, Key, Pid} | handoff(Primary, Handoff)];

handoff([{Addr, Key, Pid} | Primary], Handoff) ->
   [{primary, Addr, Key, Pid} | handoff(Primary, Handoff)];

handoff([], _) ->
   [].


%%
%% ring reconcile algorithm
reconcile(Va, A, Vb, B) ->
   case {ek_vclock:descend(Va, Vb), ek_vclock:descend(Vb, Va)} of
      %% 1. A vclock is decent of B   
      {true, _} ->
         {Va, [], []};

      %% 2. B vclock is decent of A
      {_, true} ->
         {ek_vclock:merge(Va, Vb), diff(B, A) ++ alive(A, B), diff(A, B)};

      %% 3. conflict
      {_,    _} ->
         Peers = ek_vclock:diff(Va, Vb),
         {ek_vclock:merge(Va, Vb), 
            lists:flatten([diff(X, B, A) ++ alive(X, A, B) || {X, _} <- Peers]),
            lists:flatten([diff(X, A, B) || {X, _} <- Peers])
         }
   end.

%%
%% 
diff(X, Y) ->
   gb_sets:to_list(
      gb_sets:difference(
         gb_sets:from_list([Key || {Key, _} <- X]), 
         gb_sets:from_list([Key || {Key, _} <- Y]) 
      )
   ).

diff(Peer, X, Y) ->
   gb_sets:to_list(
      gb_sets:difference(
         gb_sets:from_list([Key || {Key, {P, _, _}} <- X, P =:= Peer]), 
         gb_sets:from_list([Key || {Key, {P, _, _}} <- Y, P =:= Peer]) 
      )
   ).

%%
%%
alive(X, Y) ->
   gb_sets:to_list(
      gb_sets:intersection(
         gb_sets:from_list([Key || {Key, {_, _, Pid}} <- X, Pid =:= undefined]), 
         gb_sets:from_list([Key || {Key, {_, _, Pid}} <- Y, Pid =/= undefined]) 
      )
   ).

alive(Peer, X, Y) ->
   gb_sets:to_list(
      gb_sets:intersection(
         gb_sets:from_list([Key || {Key, {P, _, Pid}} <- X, P =:= Peer, Pid =:= undefined]), 
         gb_sets:from_list([Key || {Key, {P, _, Pid}} <- Y, P =:= Peer, Pid =/= undefined]) 
      )
   ).

