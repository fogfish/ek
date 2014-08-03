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
%%   vector clock to generate partial ordering of events
%%     http://en.wikipedia.org/wiki/Vector_clock
%%     http://zoo.cs.yale.edu/classes/cs426/2012/lab/bib/fidge88timestamps.pdf
-module(ek_vclock).

-export([
   new/0
  ,new/1
  ,inc/1
  ,inc/2
  ,merge/2
  ,descend/2
  ,descend/3
]).

-type(peer()   :: any()).
-type(vclock() :: [{peer(), integer()}]).

%%
%% create new vector clock
-spec(new/0 :: () -> vclock()).
-spec(new/1 :: (peer()) -> vclock()).

new() ->
   new(erlang:node()).

new(Node) -> 
   [{Node, 0}].

%%
%% increment vector clock
-spec(inc/1 :: (vclock()) -> vclock()).
-spec(inc/2 :: (peer(), vclock()) -> vclock()).

inc(VClock) ->
   inc(erlang:node(), VClock).

inc(Node, VClock) ->
   case lists:keytake(Node, 1, VClock) of
      false ->
         [{Node, 1} | VClock];
      {value, {Node, Value}, Tail} ->
         [{Node, Value + 1} | Tail]
   end.

%%
%% merge vector clock
-spec(merge/2 :: (vclock(), vclock()) -> vclock()). 

merge(A, B) ->
   do_merge(lists:keysort(1, A), lists:keysort(1, B)).

do_merge([{NodeA, X}|A], [{NodeB, _}|_]=B)
 when NodeA < NodeB ->
   [{NodeA, X} | do_merge(A, B)];

do_merge([{NodeA, _}|_]=A, [{NodeB, X}|B])
 when NodeA > NodeB ->
   [{NodeB, X} | do_merge(A, B)];

do_merge([{Node, X}|A], [{Node, Y}|B]) ->
   [{Node, erlang:max(X, Y)} | do_merge(A, B)];

do_merge([], B) ->
   B;

do_merge(A, []) ->
   A.

%%
%% return true if A clock is descend B : A -> B 
-spec(descend/2 :: (vclock(), vclock()) -> boolean()).

descend(_, []) ->
   true;
descend(A, [{Node, X}|B]) ->
   case lists:keyfind(Node, 1, A) of
      false ->
         (X =< 0) andalso descend(A, B);
      {_, Y}  ->
         (X =< Y) andalso descend(A, B)
   end.

%%
%% return true if A clock is descend B with an exception to give peer 
%% the method allows to discover local conflicts
-spec(descend/3 :: (peer(), vclock(), vclock()) -> boolean()).

descend(Node, A, B) ->
   descend(lists:keydelete(Node, 1, A), lists:keydelete(Node, 1, B)).


