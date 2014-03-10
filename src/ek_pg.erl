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
%%  process group topology observes changes and notifies local processes
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
handle_call({join, Pid}, _, #srv{}=S) ->
   % join the process if it is not known to group
   case dict:find(Pid, S#srv.local) of
      error      ->
         {reply, ok, join_local_process(Pid, S)};
      {ok, _Ref} ->
         {reply, ok, S}
   end;

handle_call({leave, Pid}, _, #srv{}=S) ->
   % leave process if it is know to group
   case dict:find(Pid, S#srv.local) of
      error     ->
         {reply, ok, S};         
      {ok, Ref} ->
         {reply, ok, leave_local_process(Pid, Ref, S)}
   end;

handle_call(peers, _Tx, #srv{}=S) ->
   % list all remote peers
   {reply, [Peer || {Peer, _} <- dict:to_list(S#srv.peer)], S};

handle_call(members, _Tx, #srv{}=S) ->
   % list all group members
   L =  [Pid || {Pid, _} <- dict:to_list(S#srv.local)],
   R =  [Pid || {Pid, _} <- dict:to_list(S#srv.remote)],
   {reply, L ++ R, S};


handle_call(Msg, Tx, S) ->
   unexpected_msg(Msg, Tx, S),
   {noreply, S}.

%%
%%
handle_cast(Msg, S) ->
   unexpected_msg(Msg, S),
   {noreply, S}.

%%
%%
handle_info({peerup, Node}, S) ->
   % join new unknown peer
   case dict:find(Node, S#srv.peer) of
      error      ->
         ?DEBUG("ek: pg ~s peerup ~s~n", [S#srv.name, Node]),
         {noreply, join_peer(Node, S)};
      {ok, _Ref} ->
         {noreply, S}
   end;

handle_info({nodeup, Node}, S) ->
   ?DEBUG("ek: pg ~s nodeup ~s~n", [S#srv.name, Node]),
   erlang:send({S#srv.name, Node}, {peerup, erlang:node()}),
   {noreply, S};

handle_info({nodedown, Node}, S) ->
   case dict:find(Node, S#srv.peer) of
      %% peer is not known at group
      error     ->
         {noreply, S};
      {ok, Ref} ->
         ?DEBUG("ek: pg ~s peerdown ~s~n", [S#srv.name, Node]),
         {noreply, leave_peer(Node, Ref, S)}
   end;

handle_info({'DOWN', _, _, Pid, _Reason}, S) ->
   case lookup_pid(Pid, S) of
      {local,  Ref} -> 
         {noreply, leave_local_process(Pid, Ref, S)};
      {remote, Ref} -> 
         {noreply, leave_remote_process(Pid, Ref, S)};
      {peer,   Ref} -> 
         {noreply, leave_peer(erlang:node(Pid), Ref, S)};
      _             -> 
         {noreply, S}
   end;


handle_info({join, Pid}, S) ->
   ?DEBUG("ek: pg ~s join ~p~n", [S#srv.name, Pid]),
   case dict:find(Pid, S#srv.remote) of
      %% process is not know at group
      error      ->
         {noreply, join_remote_process(Pid, S)};
      %% process is known (ignore)
      {ok, _Ref} ->
         {noreply, S}
   end;

handle_info({leave, Pid}, #srv{}=S) ->
   ?DEBUG("ek: pg ~s leave ~p~n", [S#srv.name, Pid]),
   case dict:find(Pid, S#srv.remote) of
      %% process is not know at group
      error     ->
         {noreply, S};         
      %% process is known (ignore)
      {ok, Ref} ->
         {noreply, leave_remote_process(Pid, Ref, S)}
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
join_peer(Node, S) ->
   Ref = erlang:monitor(process, {S#srv.name, Node}),
   _   = erlang:send({S#srv.name, Node}, {peerup, erlang:node()}),
   %% flush local process(es) state to remote peer
   ok  = foreach(
      fun(Pid, _Ref) -> erlang:send({S#srv.name, Node}, {join, Pid}) end,
      S#srv.local
   ),
   S#srv{
      peer = dict:store(Node, Ref, S#srv.peer)
   }.

%%
%%
leave_peer(Node, Ref, S) ->
   _   = erlang:demonitor(Ref, [flush]),
   S#srv{
      peer = dict:erase(Node, S#srv.peer)
   }.

%%
%%
join_local_process(Pid, S) ->
   Ref = erlang:monitor(process, Pid),
   ok  = send_to_peer({join, Pid}, S#srv.name, S#srv.peer), 
   ok  = foreach(
      fun(X, _) -> 
         erlang:send(X,   {join, Pid}),
         erlang:send(Pid, {join, X})
      end,
      S#srv.local
   ),
   ok  = foreach(
      fun(X, _) ->
         erlang:send(Pid, {join, X})
      end,
      S#srv.remote
   ),
   S#srv{
      local = dict:store(Pid, Ref, S#srv.local)
   }.

%%
%%
leave_local_process(Pid, Ref, S) ->
   Pids = dict:erase(Pid, S#srv.local),
   _    = erlang:demonitor(Ref, [flush]),
   ok   = send_to_pids({leave, Pid}, Pids),  
   ok   = send_to_peer({leave, Pid}, S#srv.name, S#srv.peer), 
   S#srv{
      local = Pids 
   }.

%%
%%
join_remote_process(Pid, S) ->
   Ref = erlang:monitor(process, Pid),
   ok  = send_to_pids({join, Pid}, S#srv.local),  
   S#srv{
      remote = dict:store(Pid, Ref, S#srv.remote)
   }.

%%
%%
leave_remote_process(Pid, Ref, S) ->
   _   = erlang:demonitor(Ref, [flush]),
   ok  = send_to_pids({leave, Pid}, S#srv.local),  
   S#srv{
      remote = dict:erase(Pid, S#srv.remote)
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



