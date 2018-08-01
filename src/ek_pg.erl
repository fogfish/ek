-module(ek_pg).
-include("ek.hrl").

-export([
   start_link/2,
   init/2,
   address/2,
   whois/3,
   successors/4,
   predecessors/4
]).

-record(state, {
   id = undefined :: atom()
}).

start_link(Id, Opts) ->
   ek_gen:start_link(Id, ?MODULE, Opts).

init(Id, Opts) ->
   #state{id = Id}.

address(Pids, #state{} = State) ->
   [VNode || {VNode, _} <- crdts_orset:value(Pids)].

whois(Key, Pids, #state{id = Id}) ->
   lists:map(
      fun({VNode, Pid}) -> 
         #primary{ring = Id, node = VNode, peer = Pid} 
      end,
      lists:filter(
         fun({VNode, _}) -> VNode =:= Key end, 
         crdts_orset:value(Pids)
      )
   ).

successors(N, Key, Pids, #state{id = Id}) ->
   {Pred, Succ} = lists:splitwith(
      fun({VNode, _}) -> VNode < Key end,
      crdts_orset:value(Pids)
   ),
   lists:map(
      fun({VNode, Pid}) -> 
         #primary{ring = Id, node = VNode, peer = Pid} 
      end,
      split(N, Succ ++ Pred)
   ).


predecessors(N, Key, Pids, #state{id = Id}) ->
   {Pred, Succ} = lists:splitwith(
      fun({VNode, _}) -> VNode < Key end,
      crdts_orset:value(Pids)
   ),
   lists:map(
      fun({VNode, Pid}) -> 
         #primary{ring = Id, node = VNode, peer = Pid} 
      end,
      split(N, lists:reverse(Pred) ++ lists:reverse(Succ))
   ).

split(N, List)
 when length(List) < N ->
   List;
split(N, List) ->
   element(1, lists:split(N, List)).

