-module(ek_router).
-behaviour(pipe).

-include("ek.hrl").

-export([
   start_link/3,
   init/1,
   free/2,
   handle/3
]).


%%
%%
-record(state, {
   id      = undefined :: atom(),
   routes  = undefined :: datum:ring(),
   peers   = undefined :: datum:tree()
}).

%%%------------------------------------------------------------------
%%%
%%% factory
%%%
%%%------------------------------------------------------------------   

start_link(Id, With, Opts) ->
   pipe:start_link({local, Id}, ?MODULE, [Id, With, Opts], []).

%%
init([Id, With, Opts]) ->
   pipe:ioctl(With, {attach, Id}),
   [self() ! {join, Vnode, Peer} || {Vnode, Peer} <- ek:members(With)],
   {ok, handle, 
      #state{
         id     = Id,
         routes = ring:new(Opts),
         peers  = bst:new()
      }
   }.

%%
free(_, _) ->
   ok.


%%%------------------------------------------------------------------
%%%
%%% state machine
%%%
%%%------------------------------------------------------------------   

handle({quorum, _, _}, _, State) ->
   {reply, ok, State};

handle({join, Vnode, Pid}, _, State) ->
   {reply, ok, join(scalar:s(Vnode), Pid, State)};

handle({handoff, Vnode, Pid}, _, State) ->
   {reply, ok, handoff(scalar:s(Vnode), Pid, State)};

handle({leave, Vnode, Pid}, _, State) ->
   {reply, ok, leave(scalar:s(Vnode), Pid, State)};

handle({successors, Key}, _, State) ->
   {reply, successors(Key, State), State};

handle({predecessors, Key}, _, State) ->
   {reply, predecessors(Key, State), State};

handle({whois, Vnode}, _, State) ->
   {reply, whois(Vnode, State), State};

handle(address, _, State) ->
   {reply, address(State), State}.


%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   

%%
%%
join(Vnode, Pid, #state{routes = Routes, peers = Peers} = State) ->
   State#state{
      routes = ring:join(Vnode, Routes),
      peers  = bst:insert(Vnode, Pid, Peers)
   }.

%%
%%
handoff(Vnode, _Pid, #state{peers = Peers} = State) ->
   State#state{
      peers = bst:insert(Vnode, undefined, Peers)
   }.


%%
%%
leave(Vnode, _Pid, #state{routes = Routes, peers = Peers} = State) ->
   State#state{
      routes = ring:leave(Vnode, Routes),
      peers  = bst:remove(Vnode, Peers)
   }.

%%
%%
successors(Key, #state{} = State) ->
   vnodes(siblings(fun ring:successors/3, Key, State), State).


%%
%%
predecessors(Key, #state{} = State) ->
   vnodes(siblings(fun ring:predecessors/3, Key, State), State).


%%
%%
siblings(With, Key, #state{routes = Routes, peers = Peers}) ->
   [{Addr, Vnode, Pid} || 
      {Addr, Vnode} <- With(ring:n(Routes) * ring:n(Routes) * 2, Key, Routes), 
                Pid <- [bst:lookup(Vnode, Peers)]
   ].

%%
%%
vnodes([], _) ->
   [];
vnodes(Candidates, #state{id = Id, routes = Routes}) ->
   [erlang:setelement(#primary.ring, Vnode, Id) ||
      Vnode <- vnodes_split_to_primary_and_handoff(ring:n(Routes), Candidates)].


%%
%%
vnodes_split_to_primary_and_handoff(N, [{Addr, _, _}|_] = Nodes) ->
   % Note: Vnode address is bound with a key, the address is replicated 
   %       to other primary nodes and carried to handoff 
   {Primary, Handoff} = split_to_primary_and_handoff(N, Nodes),
   sort_vnodes(
      handoff_undefined_primary(Addr, Primary, Handoff)).

%%
%% 
split_to_primary_and_handoff(N, Nodes) ->
   split_to_primary_and_handoff(N, Nodes, []).

split_to_primary_and_handoff(N, Nodes, Primary)
 when length(Primary) =:= N ->
   Handoff = lists:filter(
      fun({_, Key, _}) -> lists:keyfind(Key, 2, Primary) =:= false end,
      Nodes
   ),
   {lists:reverse(Primary), Handoff};

split_to_primary_and_handoff(N, [{_, Vnode, _}=Node | Nodes], Primary) ->
   case lists:keyfind(Vnode, 2, Primary) of
      false ->
         split_to_primary_and_handoff(N, Nodes, [Node | Primary]);
      _     ->
         split_to_primary_and_handoff(N, Nodes, Primary)
   end;

split_to_primary_and_handoff(_, [], Primary) ->
   {lists:reverse(Primary), []}.   


%%
%% 
handoff_undefined_primary(Addr, [{_, _, undefined} | _] = Primary, [{_, _, undefined} | Handoff]) ->
   handoff_undefined_primary(Addr, Primary, Handoff);

handoff_undefined_primary(Addr, [{_, _, undefined} | Primary], []) ->
   handoff_undefined_primary(Addr, Primary, []);

handoff_undefined_primary(Addr, [{_, Vnode, undefined} | Primary], [{_, _, Peer} | Handoff]) ->
   [#handoff{addr = Addr, node = Vnode, peer = Peer} | handoff_undefined_primary(Addr, Primary, Handoff)];

handoff_undefined_primary(Addr, [{_, Vnode, Peer} | Primary], Handoff) ->
   [#primary{addr = Addr, node = Vnode, peer = Peer} | handoff_undefined_primary(Addr, Primary, Handoff)];

handoff_undefined_primary(_Addr, [], _) ->
   [].

%%
%%
sort_vnodes(Nodes) ->
   case
      lists:partition(
         fun(X) -> erlang:element(1, X) =/= primary end,
         Nodes
      )
   of
      {Handoff,      []} ->
         Handoff;
      {Handoff, Primary} ->
         Primary ++ Handoff
   end.

%%
%%
whois(Vnode, #state{id = Id, routes = Routes, peers = Peers}) ->
   case bst:lookup(Vnode, Peers) of
      undefined ->
         [];
      Peer ->
         [#primary{ring = Id, addr = Addr, node = Vnode, peer = Peer} ||
            {Addr, _} <- ring:whois(Vnode, Routes)
         ]
   end.

%%
%%
address(#state{routes = Routes}) ->
   ring:address(Routes).
