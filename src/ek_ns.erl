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
  ,peer     = undefined :: datum:tree() %% list of remote peers (nodes running ns)
  ,ring     = undefined :: datum:ring() %% name-space ring
  ,vclock   = undefined :: any()        %% vector clock of the ring
  ,gossip   = undefined :: integer()    %% gossip timeout
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
   {ok, 
      #srv{
         name     = Name
        ,peer     = bst:new()
        ,ring     = ring:new(Opts)
        ,vclock   = ek_vclock:new()
        ,gossip   = proplists:get_value(gossip,   Opts, ?CONFIG_GOSSIP_TIMEOUT)
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

handle_call(peers, _Tx, State) ->
   % list all remote peers (nodes running name space leader)
   Result = [Peer || {Peer, _} <- bst:list(State#srv.peer)],
   {reply, Result, State};

handle_call(members, _Tx, State) ->
   % list all group members (processes members of ring)
   Result = [{Key, Pid} || {Key, {_, _, Pid}} <- ring:members(State#srv.ring)],
   {reply, Result, State};

handle_call({predecessors, Key}, _Tx, State) ->
   {reply, whereis(Key, fun ring:predecessors/3, State#srv.ring), State};

handle_call({successors, Key}, _Tx, State) ->
   {reply, whereis(Key, fun ring:successors/3, State#srv.ring), State};

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

handle_info(gossip, State) ->   
   erlang:send_after(State#srv.gossip, self(), gossip),
   Msg  = {reconcile, erlang:node(), State#srv.vclock, ring:members(State#srv.ring)},
   lists:foreach(
      fun(Peer) ->
         erlang:send({State#srv.name, Peer}, Msg)
      end,
      random_peer(State#srv.exchange, State)
   ),
   {noreply, State};

handle_info({reconcile, _Peer, Vpeer, Vring}, #srv{vclock=Va}=State) ->
   {VClock, Join, Leave} = reconcile(
      State#srv.vclock
     ,ring:members(State#srv.ring)
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
join_process(Id, {Peer, Pid}, State) ->
   try
      case ring:get(Id, State#srv.ring) of
         {_, _, X} when X =/= Pid ->
            join_to_ring(Id, {Peer, Pid}, State);
         _ ->
            State
      end
   catch _:badarg ->
      join_to_ring(Id, {Peer, Pid}, State)
   end.

join_to_ring(Key, {Node, undefined}, State) ->
   ?DEBUG("ek [ns]: ~s join ~p ~p~n", [State#srv.name, Key, undefined]),
   State#srv{
      ring   = ring:join(Key, {Node, undefined, undefined}, State#srv.ring)
     ,vclock = ek_vclock:inc(State#srv.vclock)
   };

join_to_ring(Key, {Peer, Pid}, State)
 when Peer =:= erlang:node() ->
   ?DEBUG("ek [ns]: ~s join ~p ~p~n", [State#srv.name, Key, Pid]),
   Ref = erlang:monitor(process, Pid),
   notify({join, Key, Pid}, State#srv.ring),
   [erlang:send(Pid, {join, K, P}) || 
      {K, {N, _, P}} <- ring:members(State#srv.ring),
                        N =:= erlang:node(),
                        P =/= undefined
   ],
   State#srv{
      ring   = ring:join(Key, {Peer, Ref, Pid}, State#srv.ring)
     ,vclock = ek_vclock:inc(State#srv.vclock)
   };

join_to_ring(Key, {Peer, Pid}, State) ->
   ?DEBUG("ek [ns]: ~s join ~p ~p~n", [State#srv.name, Key, Pid]),
   Ref = erlang:monitor(process, Pid),
   notify({join, Key, Pid}, State#srv.ring),
   State#srv{
      ring   = ring:join(Key, {Peer, Ref, Pid}, State#srv.ring)
     ,vclock = ek_vclock:inc(State#srv.vclock)
   }.

%%
%% process is explicitly left from the ring 
leave_process(Key, State) ->
   try
      ?DEBUG("ek [ns]: ~s leave ~p~n", [State#srv.name, Key]),
      case ring:get(Key, State#srv.ring) of
         {_, _, undefined} ->
            ok;
         {_, Ref, _} ->
            erlang:demonitor(Ref, [flush])
      end,
      notify({leave, Key}, State#srv.ring),
      State#srv{
         ring   = ring:leave(Key, State#srv.ring)
        ,vclock = ek_vclock:inc(State#srv.vclock)
      }
   catch _:badarg ->
      State
   end.

%%
%% transient failure of ring member
%% the "key" is still allocated by ring member but 
%% writes hand-off to another partition 
failure_process(Pid, State) ->
   ?DEBUG("ek [ns]: ~s failed ~p~n", [State#srv.name, Pid]),
   List = ring:members(State#srv.ring),
   case [X || X = {_, {_, _, P}} <- List, P =:= Pid] of
      [{Key, {Node, Ref, Pid}}] ->
         erlang:demonitor(Ref, [flush]),
         notify({handoff, Key}, State#srv.ring),
         State#srv{
            ring = ring:join(Key, {Node, undefined, undefined}, State#srv.ring)
         };
      _ ->
         State
   end.


%%
%% notify local processes
notify(Msg, Ring) ->
   lists:foreach(
      fun({_, {Node, _, Pid}}) ->
         if
            Node =:= erlang:node(), Pid =/= undefined ->
               erlang:send(Pid, Msg);
            true ->
               ok
         end
      end,
      ring:members(Ring)
   ).      


% %%
% %% reconcile changes from descent ring
% reconcile(VClock, Ring, State) ->
%    ?DEBUG("ek [ns]: ~s reconcile~n", [State#srv.name]),
%    diff(ring:members(State#srv.ring), Ring),
%    State#srv{
%       vclock = ek_vclock:merge(VClock, State#srv.vclock)
%    }.

% %%
% %% resolve conflict from incompatible ring
% %% automatic resolution is possible only for 'local conflict'
% conflict(Peer, Vb, Ring, #srv{vclock=Va}=State) ->
%    ?DEBUG("ek [ns]: ~s conflict~n",    [State#srv.name]),
%    case {ek_vclock:descend(Peer, Vb, Va), ek_vclock:descend(Peer, Va, Vb)} of
%       %% local conflict: remote ring is descent of local
%       {true, _} ->
%          local_conflict(Peer, Vb, Ring, State);

%       %% local conflict: local ring is descent of remote
%       {_, true} ->
%          local_conflict(Peer, Vb, Ring, State);

%       %% global conflict
%       {_,    _} ->
%          error_logger:error_report([
%             {conflict, State#srv.name}
%            ,{node,     [erlang:node(), Va]}
%            ,{peer,     [Peer,          Vb]}
%          ]),
%          State
%    end.

% %%
% %% resolve local conflict, peek only entity owned by peer, 
% %% use them as ground truth 
% local_conflict(Peer, VClock, Ring, State) ->
%    A = lists:filter(
%       fun({_, {X, _, _}}) -> X =:= Peer end,
%       ring:members(State#srv.ring)
%    ),
%    B = lists:filter(
%       fun({_, {X, _, _}}) -> X =:= Peer end,
%       Ring 
%    ),
%    diff(A, B),
%    State#srv{
%       vclock = ek_vclock:merge(VClock, State#srv.vclock)
%    }.


% %%
% %% calculate difference for added, removed and recovered nodes
% diff(A, B) ->
%    Sa    = gb_sets:from_list([X || {X, _} <- A]),
%    Sb    = gb_sets:from_list([X || {X, _} <- B]),
%    Ua    = gb_sets:from_list([X || {X, {_, _, Pid}} <- A, Pid =:= undefined]),
%    Ub    = gb_sets:from_list([X || {X, {_, _, Pid}} <- B, Pid =/= undefined]),
%    Join  = gb_sets:to_list(gb_sets:difference(Sb, Sa)),
%    Leave = gb_sets:to_list(gb_sets:difference(Sa, Sb)),
%    Alive = gb_sets:to_list(gb_sets:intersection(Ua, Ub)),

%    lists:foreach(
%       fun(Id) ->
%          {_, {Peer, _, Pid}} = lists:keyfind(Id, 1, B),
%          erlang:send(self(), {join, Id, {Peer, Pid}})
%       end,
%       Join ++ Alive
%    ),
%    lists:foreach(
%       fun(Id) ->
%          erlang:send(self(), {leave, Id})
%       end,
%       Leave
%    ).

%%
%% return list of nodes for key
whereis(Key0, Fun, Ring) ->
   N = ring:n(Ring),
   Nodes = [{Addr, Key, Pid} || 
      {Addr, Key} <- Fun(N * 2, Key0, Ring), 
      {_, _, Pid} <- [ring:get(Key, Ring)]
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

