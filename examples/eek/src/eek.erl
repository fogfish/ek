-module(eek).

-export([
   start/0
]).

start() ->
   application:start(ek),
   application:start(eek).
