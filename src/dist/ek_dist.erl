%% @doc
%% 
-module(ek_dist).

-export([
   dist_port/1,
   listen/1,
   select/1,
   accept/1,
   accept_connection/5,
   setup/5,
   close/1,
   childspecs/0
]).

%%
%% bits for consistent hashing
-define(M,  10).

%%
%% reserved port to run Erlang in Docker
-define(ERL_IN_DOCK, 32100). 

%%
%% distribution port
dist_port(Node)
 when is_atom(Node) ->
   dist_port(erlang:atom_to_list(Node));
dist_port(Node)
 when is_list(Node) ->
   dist_port(erlang:list_to_binary(Node));
dist_port(Node)
 when is_binary(Node) ->
   case application:get_env(kernel, inet_dist_base_port, 45000) of 
      ?ERL_IN_DOCK ->
         ?ERL_IN_DOCK;
      Base ->
         <<Addr:?M, _/bits>> = crypto:hash(sha, Node),
         Base + Addr
   end.

listen(Name) ->
   Port = dist_port(Name),
   ok = application:set_env(kernel, inet_dist_listen_min, Port),
   ok = application:set_env(kernel, inet_dist_listen_max, Port),
   inet_tcp_dist:listen(Name).

select(Node) ->
   inet_tcp_dist:select(Node).

accept(Listen) ->
   inet_tcp_dist:accept(Listen).

accept_connection(AcceptPid, Socket, MyNode, Allowed, SetupTime) ->
   inet_tcp_dist:accept_connection(AcceptPid, Socket, MyNode, Allowed, SetupTime).

setup(Node, Type, MyNode, LongOrShortNames, SetupTime) ->
   inet_tcp_dist:setup(Node, Type, MyNode, LongOrShortNames, SetupTime).

close(Listen) ->
   inet_tcp_dist:close(Listen).

childspecs() ->
   inet_tcp_dist:childspecs().
