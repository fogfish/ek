-module(ek_pg_SUITE).
-include_lib("common_test/include/ct.hrl").

%% common test
-export([
   all/0,
   groups/0,
   init_per_suite/1,
   end_per_suite/1,
   init_per_group/2,
   end_per_group/2
]).

-export([
   ek_peers_discovery/1,
   ek_process_join/1
]).
 
%% Number of cluster nodes see tests.config 
-define(CLUSTER,  5).
-define(NODES,   ['a@127.0.0.1', 'b@127.0.0.1', 'c@127.0.0.1', 'd@127.0.0.1', 'e@127.0.0.1']).

%%%----------------------------------------------------------------------------   
%%%
%%% suite
%%%
%%%----------------------------------------------------------------------------   
all() ->
   [
      {group, pg}
   ].

groups() ->
   [
      {pg, [parallel, {repeat, 1}], 
         [
            ek_peers_discovery,
            ek_process_join
         ]}
   ].

%%%----------------------------------------------------------------------------   
%%%
%%% init
%%%
%%%----------------------------------------------------------------------------   
init_per_suite(Config) ->
   ek:start(),
   Config.

end_per_suite(_Config) ->
   ok.

%% 
%%
init_per_group(_, Config) ->
   Config.

end_per_group(_, _Config) ->
   ok.


%%%----------------------------------------------------------------------------   
%%%
%%% unit tests
%%%
%%%----------------------------------------------------------------------------   

ek_peers_discovery(_) ->
   {ok, _} = ek:create(pg_peers),
   ek_pending_peers(pg_peers, ?CLUSTER - 1, 5),
   ct:pal("==> ~p~n", [ek:peers(pg_peers)]).

ek_process_join(_) ->
   {ok, _} = ek:create(pg_join),
   ek_pending_peers(pg_join, ?CLUSTER - 1, 5),
   ct:pal("==> ~p~n", [ek:peers(pg_join)]),

   ek:join(pg_join),   
   ek_pending_members(pg_join, ?CLUSTER, 5),
   ct:pal("==> ~p~n", [ek:members(pg_join)]),

   ?CLUSTER = ek:size(pg_join),
   ?CLUSTER = length(ek:members(pg_join)),
   ?NODES   = lists:sort(ek:address(pg_join)),

   Peers    = lists:sort(ek:peers(pg_join)),
   Peers    = lists:sort(ek_recv_join(?CLUSTER - 1)),
   
   timer:sleep(1000).



%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
ek_pending_peers(Cluster, N, T) ->
   ek_pending_with(fun() -> ek:peers(Cluster) end, N, T).

%%
%%
ek_pending_members(Cluster, N, T) ->
   ek_pending_with(fun() -> ek:members(Cluster) end, N, T).

%%
%%
ek_pending_with(_, _, 0) ->
   exit(timeout);

ek_pending_with(Fun, N, T) ->
   case length(Fun()) of
      X when X < N ->
         timer:sleep(1000),
         ek_pending_with(Fun, N, T - 1);
      _ ->
         ok
   end.

%%
%%
ek_recv_join(0) ->
   [];
ek_recv_join(N) ->
   receive
      {join, Addr, _Pid} ->
         [Addr | ek_recv_join(N - 1)]
   after 1000 ->
      exit(timeout)
   end.







