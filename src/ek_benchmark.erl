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
%%   ek benchmark script
-module(ek_benchmark).

-export([
   new/1
  ,run/4
]).

-define(SLAVES,  [a,b,c,d]).
-define(RING,    [
   {type,  ns}
  ,{m,     16}
  ,{n,      3}
  ,{q,   4096}
  ,{hash, sha}
]).

%%
%%
new(_Id) ->
   _ = init(),
   {ok, undefined}.

%%
%%
run(predecessors, _KeyGen, ValGen, State) ->
   _List = ek:predecessors(eek, ValGen()),
   {ok, State}.

%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   

%%
%% init application
init() ->
   case application:start(ek) of
      %% application started for first time
      %% build a cluster on slave nodes
      ok ->
         {ok, _} = ek:create(eek, ?RING),
         ok = ek:join(eek, addr(), self()),
         % lists:foreach(
         %    fun(X) -> {ok, _} = clone:start(X, [eek]) end,
         %    basho_bench_config:get(slaves, ?SLAVES)
         % ),
         ping();
      _  ->
         {ok, _} = ek:create(eek, ?RING),
         ok = ek:join(eek, addr(), self()),
         ok
   end.  

ping() ->
   case ek:members(eek) of
      [] ->
         timer:sleep(100),
         ping();
      _  ->
         ok
   end.

%%
%%
addr() ->
   random:seed(erlang:now()),
   crypto:hash(sha,
      erlang:term_to_binary({erlang:now(), random:uniform(16#ffffffff)})
   ).

