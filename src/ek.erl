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
%%   Erlang clustering utility
-module(ek).
-include("ek.hrl").

-export([start/0]).
-export([
   seed/1
  ,seed/2
]).
-export([
   create/1
  ,create/2
  ,peers/1 
  ,members/1
  ,join/1
  ,join/2
  ,join/3
  ,leave/1
  ,leave/2
  ,predecessors/2
  ,successors/2
]).

%%
-type(pg()     :: pid() | atom()).
-type(key()    :: any()).
-type(addr()   :: integer()).
-type(vnode()  :: {primary | handoff, addr(), key(), pid()}).

%%
%% start application
-ifdef(CONFIG_DEBUG).
start() ->
   application:start(sasl),
   application:start(ek).
-else.
start() ->
   error_logger:tty(false),
   application:start(sasl),
   application:start(ek).
-endif.

%%
%% seed cluster nodes
-spec(seed/1 :: ([node()]) -> {ok, pid()} | {error, any()}).
-spec(seed/2 :: ([node()], timeout()) -> {ok, pid()} | {error, any()}).

seed(Seed) ->
   seed(Seed, ?CONFIG_SEED_INTERVAL).

seed(Seed, Timeout) ->
   ek_sup:start_child(worker, erlang:make_ref(), ek_seed, [Seed, Timeout]).

%%
%% create process group topology, topology manager notifiers all processes
%% on membership changes
%%   {join,    key(), pid()} - process joined topology
%%   {handoff, key(), pid()} - process temporary failed  
%%   {leave,   key(), pid()} - process left topology 
-spec(create/1 :: (atom()) -> {ok, pid()}).
-spec(create/2 :: (atom(), list()) -> {ok, pid()}).

create(Name) ->
   create(Name, [{type, pg}]).

create(Name, Opts) ->
   case lists:keyfind(type, 1, Opts) of
      false ->
         create(ek_pg, Name, Opts);
      {type, pg} ->
         create(ek_pg, Name, Opts);
      {type, ns} ->
         create(ek_ns, Name, Opts)
   end.

create(Mod, Name, Opts) ->
   case whereis(Name) of
      undefined -> 
         ek_sup:start_child(worker, Name, Mod, [Name, Opts]);
      Pid ->
         {ok, Pid}
   end.

%%
%% list topology peers, Erlang nodes running same topology manager
-spec(peers/1 :: (pg()) -> [node()]).

peers(Name) -> 
   gen_server:call(Name, peers).

%%
%% list all topology members (processes)
-spec(members/1 :: (pg()) -> [{key(), pid()}]).

members(Name) ->
   gen_server:call(Name, members).


%%
%% join process to topology
-spec(join/1 :: (pg()) -> ok | {error, any()}).
-spec(join/2 :: (pg(), pid()) -> ok | {error, any()}).
-spec(join/3 :: (pg(), key(), pid()) -> ok | {error, any()}).

join(Name) ->
   join(Name, undefined, self()).
join(Name, Pid) ->
   join(Name, undefined, Pid).
join(Name, Key, Pid) ->
   gen_server:call(Name, {join, Key, Pid}).

%%
%% leave process from topology
-spec(leave/1 :: (pg()) -> ok | {error, any()}).
-spec(leave/2 :: (pg(), key() | pid()) -> ok | {error, any()}).

leave(Name) ->
   leave(Name, self()).
leave(Name, Uid) ->
   gen_server:call(Name, {leave, Uid}).


%%
%% return list of N predecessors processes
-spec(predecessors/2 :: (pg(), key()) -> [vnode()]).

predecessors(Name, Key) ->
   gen_server:call(Name, {predecessors, Key}).

%%
%% return list of N successors processes
-spec(successors/2 :: (pg(), key()) -> [vnode()]).

successors(Name, Key) ->
   gen_server:call(Name, {successors, Key}).




