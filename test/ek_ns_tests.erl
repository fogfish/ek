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
-module(ek_ns_tests).
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
   {node, NODE, "-boot start_clean -pa ../ebin -pa ../deps/*/ebin", {spawn, NODE, {inorder, SUITE}} }
).

%%%------------------------------------------------------------------
%%%
%%% suites
%%%
%%%------------------------------------------------------------------   

local_node_test_() ->
   {foreach,
      fun ns_init/0,
      fun ns_free/1,
      [
         fun join/1
        ,fun leave/1
      ]
   }.

peer_discovery_test_() ->
   ?mk_test([
      ?mk_node('a@127.0.0.1', peer_test())
     ,?mk_node('b@127.0.0.1', peer_test())
   ]).

peer_test() ->
   {setup,
      fun ns_init/0,
      fun ns_free/1,
      fun(X) ->
         [
            ping()
           ,join(X)
           ,members(X)
           ,leave(X)
         ]
      end
   }.

%%%------------------------------------------------------------------
%%%
%%% setup
%%%
%%%------------------------------------------------------------------   

ns_init() ->
   _  = error_logger:tty(false),
   ok = application:start(ek),
   {ok, Pid} = ek:create(test_ns, [{type, ring}, {gossip, 500}, {exchange, 1}]),
   Uid = crypto:hash(sha, erlang:term_to_binary(erlang:now())),
   {Pid, Uid}.

ns_free(_)->
   timer:sleep(1000),
   application:stop(ek).

%%%------------------------------------------------------------------
%%%
%%% unit tests
%%%
%%%------------------------------------------------------------------   

%%
%%
ping() ->
   [
      ?_assertMatch(pong, net_adm:ping(?master))
   ].

%%
%%
join({Ns, Uid}) ->
   [
      ?_assertMatch(ok, ek:join(Ns, Uid, self()))
     ,?_assertMatch(ok, timer:sleep(2000))
   ].

%%
%%
leave({Ns, Uid}) ->
   [
      ?_assertMatch(ok, ek:leave(Ns, Uid))
   ].

%%
%%
members({Ns,_Uid}) ->
   [
      ?_assertEqual(1, length(ek:peers(Ns)))
     ,?_assertEqual(2, length(ek:members(Ns)))
   ].

