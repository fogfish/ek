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
%%
%% @todo
%%  * auto discovery of ring config
%%  * drop quorum
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
  ,size/1
  ,address/1
  ,whois/2
  ,join/1
  ,join/2
  ,join/3
  ,leave/1
  ,leave/2
  ,predecessors/2
  ,successors/2
  ,vnode/2
  ,vnode/3
]).

%%
-type(pg()     :: pid() | atom()).
-type(key()    :: any()).
-type(addr()   :: integer()).
-type(vnode()  :: {primary | handoff, atom(), addr(), key(), pid()}).

%%
%% start application
-ifdef(CONFIG_DEBUG).
start() ->
   application:start(ek).
-else.
start() ->
   error_logger:tty(false),
   application:start(ek).
-endif.

%%
%% seed cluster nodes
-spec seed([node()]) -> {ok, pid()} | {error, any()}.
-spec seed([node()], timeout()) -> {ok, pid()} | {error, any()}.

seed(Seed) ->
   seed(Seed, ?CONFIG_SEED_INTERVAL).

seed(Seed, Timeout) ->
   ek_sup:start_child(worker, erlang:make_ref(), ek_seed, [Seed, Timeout]).

%%
%% create process topology manager 
%%  Options
%%   {type,      atom()} - type of topology
%%   {quorum, integer()} - topology quorum requirements
%%
%% The topology notifiers all processes on membership changes
%%   {join,    key(), pid()} - process joined topology
%%   {handoff, key()} - process temporary failed  
%%   {leave,   key()} - process left topology 
-spec create(atom()) -> {ok, pid()}.
-spec create(atom(), list()) -> {ok, pid()}.

create(Name) ->
   create(Name, [{type, pg}]).

create(Name, Opts) ->
   case lists:keyfind(type, 1, Opts) of
      false ->
         create(ek_pg, Name, Opts);
      {type, pg} ->
         create(ek_pg, Name, Opts);
      {type,  _} ->
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
-spec peers(pg()) -> [node()].

peers(Name) -> 
	gen_server:call(Name, peers).

%%
%% list all topology members (processes)
-spec members(pg()) -> [{key(), pid()}].

members(Name) ->
	gen_server:call(Name, members).

%%
%% return size of topology
-spec size(pg()) -> integer().

size(Name) ->
   gen_server:call(Name, size).
   

%%
%% list addresses managed by topology 
-spec address(pg()) -> [integer()].

address(Name) ->
	gen_server:call(Name, address).

%%
%% lists vnode allocated by key
-spec whois(pg(), key()) -> [{integer()}].

whois(Name, Key) ->
   gen_server:call(Name, {whois, Key}).

%%
%% join process to topology
-spec join(pg()) -> ok | {error, any()}.
-spec join(pg(), pid()) -> ok | {error, any()}.
-spec join(pg(), key(), pid()) -> ok | {error, any()}.

join(Name) ->
   join(Name, erlang:node(), self()).
join(Name, Pid) ->
   join(Name, erlang:node(), Pid).
join(Name, Node, Pid) ->
   gen_server:call(Name, {join, Node, Pid}).

%%
%% leave process from topology
-spec leave(pg()) -> ok | {error, any()}.
-spec leave(pg(), key() | pid()) -> ok | {error, any()}.

leave(Name) ->
   leave(Name, self()).
leave(Name, Key) ->
   gen_server:call(Name, {leave, Key}).


%%
%% return list of N predecessors processes
-spec predecessors(pg(), key()) -> [vnode()].

predecessors(Name, Key) ->
   gen_server:call(Name, {predecessors, Key}).

%%
%% return list of N successors processes
-spec successors(pg(), key()) -> [vnode()].

successors(Name, Key) ->
   gen_server:call(Name, {successors, Key}).

%%
%% return vnode attribute
-spec vnode(atom(), vnode()) -> any().

vnode(type, Vnode) -> erlang:element(1, Vnode);
vnode(ring, Vnode) -> erlang:element(2, Vnode);
vnode(addr, Vnode) -> erlang:element(3, Vnode);
vnode(node, Vnode) -> erlang:element(4, Vnode);
vnode(peer, Vnode) -> erlang:element(5, Vnode).


vnode(type, X, Vnode) -> erlang:setelement(1, Vnode, X);
vnode(ring, X, Vnode) -> erlang:setelement(2, Vnode, X);
vnode(addr, X, Vnode) -> erlang:setelement(3, Vnode, X);
vnode(node, X, Vnode) -> erlang:setelement(4, Vnode, X);
vnode(peer, X, Vnode) -> erlang:setelement(5, Vnode, X).



