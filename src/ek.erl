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
%% @doc
%%   Erlang clustering utility
-module(ek).
-include("ek.hrl").

-export([start/0]).

-export([
   seed/1,
   seed/2
]).

-export([
   pg/1,
   pg/2,
   peers/1,
   members/1,
   join/1,
   join/2,
   join/3,
   leave/1,
   leave/2
]).

-export([
   router/3,
   successors/2,
   predecessors/2,
   whois/2,
   address/1,
   vnode/2,
   vnode/3
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
   application:ensure_all_started(?MODULE).
-else.
start() ->
   error_logger:tty(false),
   application:ensure_all_started(?MODULE).
-endif.

%%
%% seed cluster nodes
-spec seed([node()]) -> datum:either( pid() ).
-spec seed([node()], timeout()) -> datum:either( pid() ).

seed(Seed) ->
   seed(Seed, ?CONFIG_SEED_INTERVAL).

seed(Seed, Timeout) ->
   ek_sup:start_child(worker, erlang:make_ref(), ek_seed, [Seed, Timeout]).


%%
%% create process group
%%  Options
%%    {gossip,   integer()} - gossip timeout 
%%    {exchange, integer()} - number of nodes to exchange gossip
%%    {quorum,   #{peers => integer(), vnode => integer()}} - number of active peers (peers quorum)
%%
%% The process group notifiers all processes on membership changes
%%   {join,    key(), pid()} - process joined topology
%%   {handoff, key(), pid()} - process temporary failed  
%%   {leave,   key(), pid()} - process left topology 
%%   {quorum,  peers | vnode, true | false}
-spec pg(atom()) -> {ok, pid()}.
-spec pg(atom(), list()) -> {ok, pid()}.

pg(Name) ->
   pg(Name, []).

pg(Name, Opts) ->
   case ek_sup:start_child(worker, Name, ek_pg, [Name, Opts]) of
      {ok, Pid} -> 
         {ok, Pid};
      {error, {already_started, Pid}} ->
         {ok, Pid};
      {error, Reason} ->
         {error, Reason}
   end.

%%
%% list group peers, Erlang nodes running same group
-spec peers(pg()) -> [node()].

peers(Name) -> 
   gen_server:call(Name, peers).

%%
%% list all topology members (processes)
-spec members(pg()) -> [{key(), pid()}].

members(Name) ->
   gen_server:call(Name, members).

%%
%% join process to group
-spec join(pg()) -> ok | {error, any()}.
-spec join(pg(), pid()) -> ok | {error, any()}.
-spec join(pg(), key(), pid()) -> ok | {error, any()}.

join(Name) ->
   join(Name, erlang:node(), self()).
join(Name, Pid) ->
   join(Name, erlang:node(), Pid).
join(Name, VNode, Pid) ->
   gen_server:call(Name, {join, VNode, Pid}).

%%
%% leave process from group
-spec leave(pg()) -> ok | {error, any()}.
-spec leave(pg(), key() | pid()) -> ok | {error, any()}.

leave(Name) ->
   leave(Name, self()).
leave(Name, Key) ->
   gen_server:call(Name, {leave, Key}).



%%
%% create routing table and bind it with a process group
%% Options
%%   {m,    integer()}  - ring module power of 2 is required
%%   {n,    integer()}  - number of replicas
%%   {q,    integer()}  - number of shard 
%%   {hash, md5 | sha1} - ring hashing algorithm
router(Name, With, Opts) ->
   case whereis(Name) of
      undefined -> 
         ek_sup:start_child(worker, Name, ek_router, [Name, With, Opts]);
      Pid ->
         {ok, Pid}
   end.

%%
%% return list of N successors processes
-spec successors(pg(), key()) -> [vnode()].

successors(Name, Key) ->
   gen_server:call(Name, {successors, Key}).

%%
%% return list of N predecessors processes
-spec predecessors(pg(), key()) -> [vnode()].

predecessors(Name, Key) ->
   gen_server:call(Name, {predecessors, Key}).

%%
%% lists vnode allocated by key
-spec whois(pg(), key()) -> [vnode()].

whois(Name, Key) ->
   gen_server:call(Name, {whois, Key}).

 
%%
%% list addresses managed by router
-spec address(pg()) -> [_].

address(Name) ->
   gen_server:call(Name, address).


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



