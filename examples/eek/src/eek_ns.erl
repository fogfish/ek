-module(eek_ns).
-behaviour(gen_server).

-export([
   start_link/0
  ,init/1
  ,terminate/2
  ,handle_call/3
  ,handle_cast/2
  ,handle_info/2
  ,code_change/3
]).

%%
%% internal state
-record(srv, {}).

%%%------------------------------------------------------------------
%%%
%%% factory
%%%
%%%------------------------------------------------------------------   

start_link() ->
   gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
   {ok, _} = ek:create(eek, [
      {type,  ns}
     ,{m,     16}
     ,{n,      3}
     ,{q,   4096}
     ,{hash, sha}
   ]),
   ok = ek:join(eek, addr(), self()),
   {ok, #srv{}}.

terminate(_, _S) ->
   ok.

%%%------------------------------------------------------------------
%%%
%%% gen_server
%%%
%%%------------------------------------------------------------------   

%%
%%
handle_call(_Msg, _Tx, State) ->
   {noreply, State}.

%%
%%
handle_cast(_Msg, State) ->
   {noreply, State}.

%%
%%
handle_info({join, Key, Pid}, State) ->
   error_logger:error_report([{join, {Key, Pid}}]),
   {noreply, State};

handle_info({handoff, Key}, State) ->
   error_logger:error_report([{handoff, Key}]),
   {noreply, State};

handle_info({leave, Key}, State) ->
   error_logger:error_report([{leave, Key}]),
   {noreply, State};

handle_info(_Msg, State) ->
   {noreply, State}.   

%%
%%
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.

%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   

%%
%% read node address from 
addr() ->
   crypto:hash(sha, erlang:term_to_binary(erlang:node())).

