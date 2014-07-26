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
%%   process group topology observes changes and notifies local processes
-module(ek_pg).
-behaviour(gen_server).
-include("ek.hrl").

-export([
   start_link/1
  ,init/1
  ,terminate/2
  ,handle_call/3
  ,handle_cast/2
  ,handle_info/2
  ,code_change/3
]).

%% internal state
-record(srv, {
   name   = undefined :: atom()   %% process group name
  ,peer   = undefined :: [node()] %% remote peers
  ,local  = undefined :: any()    %% local process dictionary     
  ,remote = undefined :: any()    %% remote process dictionary 
}).

%%%------------------------------------------------------------------
%%%
%%% factory
%%%
%%%------------------------------------------------------------------   

start_link(Name) ->
   gen_server:start_link({local, Name}, ?MODULE, [Name], []).

init([Name]) ->
   Node = erlang:node(),
   %% notify all known nodes that group peer is up
   [erlang:send({Name, X}, {peerup, Node}) || X <- erlang:nodes()], 
   _ = net_kernel:monitor_nodes(true),
   {ok, 
      #srv{
         name   = Name
        ,peer   = dict:new()
        ,local  = dict:new()
        ,remote = dict:new()
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
handle_call({join, Id, Pid}, _, #srv{}=State) ->
   % join the process if it is not known to group
   case dict:find(Pid, State#srv.local) of
      error      ->
         {reply, ok, join_local_process(Id, Pid, State)};
      {ok, _Ref} ->
         {reply, ok, State}
   end;

handle_call({leave, Pid}, _, #srv{}=State) ->
   % leave process if it is know to group
   case dict:find(Pid, State#srv.local) of
      error     ->
         {reply, ok, State};         
      {ok, Ref} ->
         {reply, ok, leave_local_process(Pid, Ref, State)}
   end;

handle_call(peers, _Tx, #srv{}=State) ->
   % list all remote peers (nodes)
   {reply, [Peer || {Peer, _} <- dict:to_list(State#srv.peer)], State};

handle_call(members, _Tx, #srv{}=State) ->
   % list all group members (processes)
   L =  [Pid || {Pid, _} <- dict:to_list(State#srv.local)],
   R =  [Pid || {Pid, _} <- dict:to_list(State#srv.remote)],
   {reply, L ++ R, State};

handle_call(Msg, Tx, State) ->
   unexpected_msg(Msg, Tx, State),
   {noreply, State}.

%%
%%
handle_cast(Msg, State) ->
   unexpected_msg(Msg, State),
   {noreply, State}.

%%
%%
handle_info({peerup, Node}, State) ->
   % join new unknown peer
   case dict:find(Node, State#srv.peer) of
      error      ->
         ?DEBUG("ek: pg ~s peerup ~s~n", [State#srv.name, Node]),
         {noreply, join_peer(Node, State)};
      {ok, _Ref} ->
         {noreply, State}
   end;

handle_info({nodeup, Node}, State) ->
   ?DEBUG("ek: pg ~s nodeup ~s~n", [State#srv.name, Node]),
   erlang:send({State#srv.name, Node}, {peerup, erlang:node()}),
   {noreply, State};

handle_info({nodedown, Node}, State) ->
   case dict:find(Node, State#srv.peer) of
      %% peer is not known at group
      error     ->
         {noreply, State};
      {ok, Ref} ->
         ?DEBUG("ek: pg ~s peerdown ~s~n", [State#srv.name, Node]),
         {noreply, leave_peer(Node, Ref, State)}
   end;

handle_info({'DOWN', _, _, Pid, _Reason}, State) ->
   case lookup_pid(Pid, State) of
      {local,  Ref} -> 
         {noreply, leave_local_process(Pid, Ref, State)};
      {remote, Ref} -> 
         {noreply, leave_remote_process(Pid, Ref, State)};
      {peer,   Ref} -> 
         {noreply, leave_peer(erlang:node(Pid), Ref, State)};
      _             -> 
         {noreply, State}
   end;


handle_info({join, Id, Pid}, State) ->
   ?DEBUG("ek: pg ~s join ~p~n", [State#srv.name, Pid]),
   case dict:find(Pid, State#srv.remote) of
      %% process is not know at group
      error      ->
         {noreply, join_remote_process(Id, Pid, State)};
      %% process is known (ignore)
      {ok, _Ref} ->
         {noreply, State}
   end;

handle_info({leave, _Id, Pid}, #srv{}=State) ->
   ?DEBUG("ek: pg ~s leave ~p~n", [State#srv.name, Pid]),
   case dict:find(Pid, State#srv.remote) of
      %% process is not know at group
      error     ->
         {noreply, State};         
      %% process is known (ignore)
      {ok, Ref} ->
         {noreply, leave_remote_process(Pid, Ref, State)}
   end;

handle_info(Msg, S) ->
   unexpected_msg(Msg, S),
   {noreply, S}.   

%%
%%
code_change(_OldVsn, S, _Extra) ->
   {ok, S}.

%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   

unexpected_msg(Msg, S) ->
   unexpected_msg(Msg, undefined, S).
unexpected_msg(Msg, Tx, S) ->
   error_logger:warning_report([
      {title,  "unexpected message"}
     ,{group,  S#srv.name}
     ,{msg,    Msg}
     ,{tx,     Tx}
   ]).

%%
%% 
join_peer(Node, State) ->
   Ref = erlang:monitor(process, {State#srv.name, Node}),
   _   = erlang:send({State#srv.name, Node}, {peerup, erlang:node()}),
   %% flush local process(es) state to remote peer
   ok  = foreach(
      fun(Pid, {Id, _Ref}) -> 
         erlang:send({State#srv.name, Node}, {join, Id, Pid}) 
      end,
      State#srv.local
   ),
   State#srv{
      peer = dict:store(Node, Ref, State#srv.peer)
   }.

%%
%%
leave_peer(Node, Ref, State) ->
   _   = erlang:demonitor(Ref, [flush]),
   State#srv{
      peer = dict:erase(Node, State#srv.peer)
   }.

%%
%%
join_local_process(Id, Pid, State) ->
   Ref = erlang:monitor(process, Pid),
   ok  = send_to_peer({join, Id, Pid}, State#srv.name, State#srv.peer), 
   ok  = foreach(
      fun(XPid, {XId, _}) -> 
         erlang:send(XPid, {join,  Id,  Pid}),
         erlang:send( Pid, {join, XId, XPid})
      end,
      State#srv.local
   ),
   ok  = foreach(
      fun(XPid, {XId, _}) ->
         erlang:send(Pid,  {join, XId, XPid})
      end,
      State#srv.remote
   ),
   State#srv{
      local = dict:store(Pid, {Id, Ref}, State#srv.local)
   }.

%%
%%
leave_local_process(Pid, {Id, Ref}, State) ->
   Pids = dict:erase(Pid, State#srv.local),
   _    = erlang:demonitor(Ref, [flush]),
   ok   = send_to_pids({leave, Id, Pid}, Pids),  
   ok   = send_to_peer({leave, Id, Pid}, State#srv.name, State#srv.peer), 
   State#srv{
      local = Pids 
   }.

%%
%%
join_remote_process(Id, Pid, State) ->
   Ref = erlang:monitor(process, Pid),
   ok  = send_to_pids({join, Id, Pid}, State#srv.local),  
   State#srv{
      remote = dict:store(Pid, {Id, Ref}, State#srv.remote)
   }.

%%
%%
leave_remote_process(Pid, {Id, Ref}, State) ->
   _   = erlang:demonitor(Ref, [flush]),
   ok  = send_to_pids({leave, Id, Pid}, State#srv.local),  
   State#srv{
      remote = dict:erase(Pid, State#srv.remote)
   }.


%%
%%
lookup_pid(Pid, S) -> 
   maybe_lookup_pid(local, Pid, S).

maybe_lookup_pid(local, Pid, S) ->
   case dict:find(Pid, S#srv.local) of
      {ok, Ref} -> {local, Ref};
      error     -> maybe_lookup_pid(remote, Pid, S)
   end;
maybe_lookup_pid(remote, Pid, S) ->
   case dict:find(Pid, S#srv.remote) of
      {ok, Ref} -> {remote, Ref};
      error     -> maybe_lookup_pid(peer, Pid, S)
   end;
maybe_lookup_pid(peer, Pid, S) ->
   case dict:find(Pid, S#srv.peer) of
      {ok, Ref} -> {peer, Ref};
      error     -> undefined
   end.

%%
%% foreach process in dictionary 
foreach(Fun, Dict) ->
   dict:fold(
      fun(Pid, Ref, Acc) -> Fun(Pid, Ref), Acc end,
      ok,
      Dict
   ).

%%
%% send message to local processes
send_to_pids(Msg, Pids) ->
   foreach(
      fun(Pid, _Ref) -> erlang:send(Pid, Msg) end,
      Pids
   ).

%%
%% send message to remote peers
send_to_peer(Msg, Group, Peers) ->
   foreach(
      fun(Peer, _Ref) -> erlang:send({Group, Peer}, Msg) end,
      Peers
   ).



