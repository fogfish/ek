%% @description
%%
-module(eek_app).
-behaviour(application).

-export([
   start/2, stop/1
]).

start(_Type, _Args) -> 
   {ok, _} = ek:seed(['ek@127.0.0.1']),
   eek_sup:start_link().

stop(_State) ->
   ok.