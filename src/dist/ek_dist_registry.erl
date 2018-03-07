%% @doc
%%
-module(ek_dist_registry).

-export([
   start_link/0,
   register_node/2,
   register_node/3,
   port_please/2,
   names/1
]).

%% The distribution protocol version number has been 5 ever since Erlang/OTP R6.
-define(VERSION, 5).

start_link() ->
   ignore.


register_node(Name, Port) ->
   Path = to_binary(io_lib:format("/nodes/~s", [Name])),
   Pckt = to_binary(Port),

   case ek_dist_http:put(Path, Pckt) of
      {ok, _} ->
         {ok, rand:uniform(3)};
      {error, econnrefused} ->
         {ok, rand:uniform(3)};
      {error, _} = Error ->
         Error
   end.

register_node(Name, Port, _Family) ->
   register_node(Name, Port).


port_please(Name, IP) ->
   Path = to_binary(io_lib:format("/nodes/~s", [Name])),
   case ek_dist_http:get(IP, Path) of
      {ok, Port} ->
         {port, scalar:i(Port), ?VERSION};
      {error, econnrefused} ->
         %% falls back to consistent hashing
         {port, ek_dist:dist_port(Name), ?VERSION};
      {error, _} = Error ->
         Error
   end.

names(Hostname) ->
   %% Erlang Port Mapper Daemon (epmd) knows neighbor peers.
   %% We ignore peer discovery via epmd
   {error, address}.


to_binary(Value)
 when is_atom(Value) ->
   to_binary(erlang:atom_to_list(Value));
to_binary(Value)
 when is_list(Value) ->
   to_binary(erlang:iolist_to_binary(Value));
to_binary(Value) 
 when is_integer(Value) ->
   to_binary(erlang:integer_to_list(Value));
to_binary(Value)
 when is_binary(Value) ->
   Value.

