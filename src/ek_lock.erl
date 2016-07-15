%% @description
%%    Lamport clock
%%    http://research.microsoft.com/en-us/um/people/lamport/pubs/time-clocks.pdf
%%
%%    draft should not be used for production
%%
-module(ek_lock).
-behaviour(gen_server).
-include("ek.hrl").

-export([
   start_link/1, start_link/2,
   lease/2, release/2,
   init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2, code_change/3
]).

-record(srv, {
   ns           :: any(),       % name space
   clock   = 0  :: integer(),   % local process clock
   q       = [] :: list(),      % request queue
   owner        :: any(),       % reference to client process
   deadline     :: any(),       % deadline to acquire a lock
   token        :: any()        % token
}).

-define(CLOCK_SEED, 30).

%%
%% start global lock manager
-spec start_link(any()) -> {ok, pid()} | {error, any()}.
-spec start_link(any(), any()) -> {ok, pid()} | {error, any()}.

start_link(Ns) ->
   gen_server:start_link(?MODULE, [Ns, undefined], []).

start_link(Ns, Token) ->
   gen_server:start_link(?MODULE, [Ns, Token], []).

init([Ns, Token]) ->
   random:seed(erlang:now()),
   ek:join(Ns, self()),
   {ok, 
      #srv{
         ns     = Ns,
         clock  = random:uniform(?CLOCK_SEED),
         token  = Token
      }
   }.

%%
%%
terminate(_, _) ->
   ok.

%%%------------------------------------------------------------------
%%%
%%% api
%%%
%%%------------------------------------------------------------------   

%%
%% leases global lock and return associated token
-spec lease(atom(), integer()) -> any() | conflict | timeout.

lease(Name, Timeout) ->
   try
      gen_server:call(Name, {lease, Timeout}, Timeout)
   catch 
      exit:{timeout, _} -> timeout
   end.

%%
%% release global lock and inject a new token
-spec release(atom(), any()) -> ok | {error, any()}.

release(Name, Token) ->
   gen_server:call(Name, {release, Token}).


%%%------------------------------------------------------------------
%%%
%%% gen_server
%%%
%%%------------------------------------------------------------------   

%%
%%
handle_call({lease, Timeout}, Tx, S0) ->
   case maybe_lease_request(usec(), Tx, Timeout, S0) of
      conflict -> 
         {reply, conflict, S0};
      S1       ->
         {noreply, S1}
   end;

handle_call({release, Token}, Tx, S) ->
   gen_server:reply(Tx, ok),
   {noreply, 
      release_global_lock(
         S#srv{
            deadline = undefined, 
            token    = Token
         }
      )
   };

handle_call(_, _, S) ->
   {noreply, S}.

%%
%%
handle_cast(_, S) ->
   {noreply, S}.

%%
%%
handle_info({join, _Ns, Pid},  #srv{clock=Clk, token=Token}=S) ->
   % new peer is joined while lock is requested, we have to request lock from it
   case is_lock_requester(self(), S) of
      true  ->
         ek:send(Pid, {lsyn, Clk, self(), Token}),
         ek:send(Pid, {lreq, Clk, self(), Token});
      false ->
         ek:send(Pid, {lsyn, Clk, self(), Token})
   end,
   {noreply, clock(S)};

handle_info({leave, _Ns, Pid}, #srv{q=Q}=S) ->
   {noreply, 
      check_global_lock(
         S#srv{
            q = shrink_queue(filter_queue(Pid, Q))
         }
      )
   };

handle_info({lreq, T, _, _}=Msg, S) ->
   {noreply, ack_global_lock(Msg, clock(T, S))};

handle_info({lack, T, _,_Token}=Msg, S) ->
   {noreply, maybe_global_lock(Msg, clock(T, S))};

handle_info({lrst, T, _, Token}=Msg, S) ->
   {noreply, 
      reset_global_lock(Msg, 
         clock(T, learn_token(T, Token, S))
      )
   };

handle_info({lsyn, T, _, Token}=Msg, S) ->
   % TODO: maybe token merge if local token is defined
   {noreply, clock(T, learn_token(T, Token, S))};

handle_info(_, S) ->
   {noreply, S}.   

%%
%%
code_change(_OldVsn, S, _Extra) ->
   {ok, S}.

%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   

%%
%%
maybe_lease_request(_Now, Tx, Timeout, #srv{deadline=undefined}=S) ->
   request_global_lock(
      S#srv{
         owner    = Tx,
         deadline = Timeout * 1000 + usec()
      }
   );   

maybe_lease_request(Now, Tx, Timeout, #srv{deadline=Deadline}=S)
 when Now > Deadline ->
   % active lock request is expired, cancel it and invalidate token
   maybe_lease_request(Now, Tx, Timeout,
      release_global_lock(
         S#srv{
            deadline = undefined%, 
            %token    = undefined (not sure if we have to invalidate token)
         }
      )
   );

maybe_lease_request(_Now, _Tx, _Timeout, _S) ->
   conflict.


%% 1. To request the resource, process Pi sends the message Tm:Pi 
%% requests resource to every other process, and puts that message 
%% on its request queue, where Tm is the time-stamp of the message.
request_global_lock(#srv{clock=Clk, ns=Ns, q=Q, token=Token}=S) ->
   S1 = clock(
      S#srv{
         q = [ek:broadcast(Ns, {lreq, Clk, self(), Token}) | Q]
      }
   ),
   case ek:members(Ns) of
      % only one member (grant lock)
      [_] -> check_global_lock(S1);
      _   -> S1
   end.

%% 3. To release the resource, process Pi removes any Tm:Pi requests
%% resource message from its request queue and sends a (timestamped) 
%% Pi releases resource message to every other process.
release_global_lock(#srv{clock=Clk, ns=Ns, q=Q, token=Token}=S) ->
   ek:broadcast(Ns, {lrst, Clk, self(), Token}),
   clock(
      S#srv{
         q = filter_queue(self(), Q)
      }
   ).

%% 2. When process Pj receives the message Tm:Pi requests resource, 
%% it places it on its request queue and sends a (timestamped) 
%% acknowledgment message to Pi.
ack_global_lock({lreq, _, Pid, _}=Req, #srv{clock=Clk, q=Q, token=Token}=S) ->
   ek:send(Pid, {lack, Clk, self(), Token}),
   S#srv{
      q = [Req | Q]
   }.

%% When process Pj receives a Pi releases resource message, it removes any Tm:Pi requests resource message 
%% from its request queue.
reset_global_lock({lrst, _, Pid, _}=Msg, #srv{q=Q}=S) ->
   check_global_lock(
      S#srv{
         q = shrink_queue(filter_queue(Pid, [Msg | Q]))
      }
   ).

%% 5. Process Pi is granted the resource when the following two 
%% conditions are satisfied: 
%% (i) There is a Tm:Pi requests resource message in its request
%% queue which is ordered before any other request in its queue 
%% by the relation =>. (To define the relation => for messages, 
%% we identify a message with the event of sending it.) 
%% (ii) Pi has received a message from every other process 
%% timestamped later than Tm.
maybe_global_lock({lack, _, _, _}=Msg, #srv{q=Q}=S) ->
   check_global_lock(
      S#srv{
         q = shrink_queue([Msg | Q])
      }
   ).

check_global_lock(#srv{ns=Ns, q=Q}=S) ->
   Nodes = [ X || X <- ek:members(Ns), X =/= self()],
   case is_lock_accured(self(), Q, Nodes) of
      true  -> lock_accured(usec(), S);
      false -> S
   end.

is_lock_accured(Self, [{lreq, _, Self, _} | Tail], Peers) ->
   is_peer_msg(Tail, Peers);

is_lock_accured(_Self, _Q, _Peers) ->
   false.

is_peer_msg(_, []) ->
   true;
is_peer_msg([], _) ->
   false;
is_peer_msg([{_, _, Peer, _} | Tail], Peers) ->
   is_peer_msg(Tail, lists:delete(Peer, Peers)).


lock_accured(Now, #srv{owner=Tx, deadline=Deadline, token=Token}=S)
 when Now < Deadline ->
   gen_server:reply(Tx, Token),
   S;
lock_accured(_, S) ->  
   release_global_lock(S#srv{owner=undefined}).


%% IR1:
%% Each process Pi increments Ci between any two successive events.
clock(#srv{clock=Clock}=S) ->
   S#srv{
      clock = Clock + 1
   }.

%% IR2:
%%  - If event a is the sending of a message m by process Pi, then the message m contains a timestamp Tm=Ci(a). 
%%  - Upon receiving a message m, process Pi sets Ci greater than or equal to its present value and greater than Tm.
clock(Time, #srv{clock=Clock}=S) ->
   S#srv{
      clock = erlang:max(Clock, Time + 1)
   }.

%%
%%
learn_token(_, undefined, S) ->
   S;
learn_token(_, Token,     S) ->
   S#srv{
      token = Token
   }.


%% filter out: lreq from process
not_lreq(Pid, {lreq, _, Pid, _}) ->
   false;
not_lreq(_, _) ->
   true. 

%% filter out: not lreq message
not_lreq({Type, _, _, _}) -> 
   Type =/= lreq.

%%
%%
shrink_queue(Q) ->
   lists:dropwhile(fun not_lreq/1, lists:keysort(2, Q)).

filter_queue(Pid, Q) ->
   lists:filter(fun(X) -> not_lreq(Pid, X) end, Q).

%%
%%
usec() ->
   {Mega, Sec, Micro} = erlang:now(),
   (Mega * 1000000 + Sec) * 1000000 + Micro.

%%
%% check if the peer is lock requester
is_lock_requester(Pid, #srv{q=Q}) ->
   case shrink_queue(Q) of
      [{lreq, _, Pid, _} | _] ->
         true;
      _ ->
         false
   end.
