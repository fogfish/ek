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
%%   
-module(ek).
-include("ek.hrl").

-export([start/0]).
-export([
   pg/1
  ,peers/1 
  ,members/1
  ,join/1
  ,join/2
  ,leave/1
  ,leave/2
]).

%%
%% start application
start() ->
   application:start(ek).


%%
%% create process group topology
-spec(pg/1 :: (atom()) -> {ok, pid()}).

pg(Name) ->
   case whereis(Name) of
      undefined -> 
         ek_sup:start_child(worker, Name, ek_pg, [Name]);
      Pid ->
         {ok, Pid}
   end.

%%
%% list topology peers
-spec(peers/1 :: (atom()) -> [node()]).

peers(Name) -> 
   gen_server:call(Name, peers).

%%
%% list topology members
-spec(members/1 :: (atom()) -> [pid()]).

members(Name) ->
   gen_server:call(Name, members).


%%
%% join topology
-spec(join/1 :: (atom()) -> ok | {error, any()}).
-spec(join/2 :: (atom(), pid()) -> ok | {error, any()}).

join(Name) ->
   join(Name, self()).
join(Name, Proc) ->
   gen_server:call(Name, {join, Proc}).

%%
%% leave topology
-spec(leave/1 :: (atom()) -> ok | {error, any()}).
-spec(leave/2 :: (atom(), pid()) -> ok | {error, any()}).

leave(Name) ->
   leave(Name, self()).
leave(Name, Pid) ->
   gen_server:call(Name, {leave, Pid}).


