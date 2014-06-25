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
%% @description
%%   the process seeds cluster configuration
-module(ek_seed).
-behaviour(gen_server).

-export([
   start_link/2,
   init/1, 
   terminate/2,
   handle_call/3, 
   handle_cast/2, 
   handle_info/2,
   code_change/3
]).

%% internal state
-record(srv, {
   seed = []        :: list()     %% list of seed nodes
  ,tts  = undefined :: timeout()  %% time-to-seed timeout
}).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

%%
%% 
start_link(Seed, Timeout) ->
   gen_server:start_link(?MODULE, [Seed, Timeout], []).

init([Seed, Timeout]) ->
   _ = erlang:send(self(), seed),
   {ok, 
      #srv{
         seed = [X || X <- Seed, X =/= erlang:node()]
        ,tts  = Timeout
      }
   }.

terminate(_, _) ->
   ok.

%%%----------------------------------------------------------------------------   
%%%
%%% gen_server
%%%
%%%----------------------------------------------------------------------------   

%%
%%
handle_call(_, _, State) ->
   {noreply, State, hibernate}.

%%
%%
handle_cast(_, State) ->
   {noreply, State, hibernate}.

%%
%%
handle_info(seed, State) ->
   seed_cluster(erlang:nodes(), State#srv.seed),
   _ = erlang:send_after(State#srv.tts, self(), seed),
   {noreply, State, hibernate};

handle_info(_, State) ->
   {noreply, State, hibernate}.

%%
%%
code_change(_Vsn, State, _) ->
   {ok, State}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
seed_cluster(Known, Seed) ->
   lists:foreach(
      fun(Node) ->
         case lists:member(Node, Known) of
            %% seed node is known
            true  ->
               alarm_handler:clear_alarm({seed, Node});
            %% seed node is unknown
            false ->
               case net_kernel:connect_node(Node) of
                  true  ->
                     ok;
                  _     ->
                     alarm_handler:clear_alarm({seed, Node}),
                     alarm_handler:set_alarm({{seed, Node}, no_connection})
               end
         end
      end,
      Seed
   ).

