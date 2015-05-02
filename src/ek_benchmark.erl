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

%%
%%
new(Id) ->
   application:start(ek),
   {ok, _} = ek:create(eek, basho_bench_config:get(ring, [])),
   Node    = erlang:list_to_binary(lists:flatten([$a + Id - 1, $@, "127.0.0.1"])),
   ok = ek:join(eek, Node, self()),
   {ok, undefined}.

%%
%%
run(predecessors, KeyGen, _ValGen, State) ->
   Key = itos(KeyGen()),
   case ek:predecessors(eek, Key) of
      List when length(List) < 3 ->
         % error_logger:error_report([{quorum, Key}, {peers, List}]),
         {error, quorum, State};
      _ ->
         {ok, State}
   end.

%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   

itos(X) ->
   erlang:list_to_binary(erlang:integer_to_list(X)).

