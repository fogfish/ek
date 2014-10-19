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
%%   see http://www.erlang.org/doc/apps/eunit/chapter.html
-module(ek_pg_tests).
-include_lib("eunit/include/eunit.hrl").

%%
%% helper macro to run multi-node unit test
-define(master, 'eunit@127.0.0.1').
-define(mk_test(SUITE), 
   {setup,
      fun() -> net_kernel:start([?master, longnames]) end,
      {inparallel, SUITE}
   }
).
-define(mk_node(NODE, SUITE), 
   {node, NODE, "-boot start_clean -pa ../ebin", {spawn, NODE, {inorder, SUITE}} }
).

%%%------------------------------------------------------------------
%%%
%%% suites
%%%
%%%------------------------------------------------------------------   

local_node_test_() ->
   {setup,
      fun pg_init/0,
      fun pg_free/1,
      [
         {"join",     fun pg_join/0}
        ,{"re-join",  fun pg_join/0}
        ,{"leave",    fun pg_leave/0}
        ,{"re-leave", fun pg_leave/0}
      ]
   }.

peer_discovery_test_() ->
   ?mk_test([
      ?mk_node('a@127.0.0.1', peer_discovery())
     ,?mk_node('b@127.0.0.1', peer_discovery())
   ]).

peer_discovery() ->
   {setup, 
      fun pg_init/0, 
      fun pg_free/1, 
      [
         fun pg_ping/0,
         fun pg_peers/0
      ]
   }.

pid_discovery_test_() ->
   ?mk_test([
      ?mk_node('a@127.0.0.1', pid_discovery())
     ,?mk_node('b@127.0.0.1', pid_discovery())
   ]).

pid_discovery() ->
   {setup, 
      fun pg_init/0, 
      fun pg_free/1, 
      [
         {"ping nodes", fun pg_ping/0},
         {"join pg",    fun pg_join/0},
         {"recv join",  fun pg_recv_join/0},
         {"leave pg",   fun pg_leave/0}
         % {"recv leave", fun pg_recv_leave/0}
      ]
   }.

%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   

pg_init() ->
   _  = error_logger:tty(false),
   ok = application:start(ek),
   {ok, Pid} = ek:create(test_pg),
   Pid.

pg_free(_)->
   timer:sleep(1000),
   application:stop(ek).

pg_ping() ->
   pong   = net_adm:ping(?master),
   %% wait for cluster
   timer:sleep(2000),
   [_, _] = erlang:nodes().

pg_peers() ->
   Nodes = [X || X <- erlang:nodes(), X =/= ?master],
   Nodes = ek:peers(test_pg).

pg_join() ->
   ok = ek:join(test_pg).

pg_leave() ->
   ok = ek:leave(test_pg).

pg_recv_join() ->
   receive
      {join, _Id, _Pid} ->
         ok
   end.

pg_recv_leave() ->
   receive
      {leave, _Id, _Pid} ->
         ok
   end.


