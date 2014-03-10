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
%%   
-module(ek_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).
-export([
   start_child/2
  ,start_child/3
  ,start_child/4
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, permanent, 5000, Type, dynamic}).


%%
%%
start_link() ->
   supervisor:start_link({local, ?MODULE}, ?MODULE, []).
   
init([]) ->   
   {ok,
      {
         {one_for_one, 4, 1800},
         []
      }
   }.

%%
%%
start_child(Type, I) -> 
   supervisor:start_child(?MODULE, ?CHILD(Type, I)).

start_child(Type, I, Args) -> 
   supervisor:start_child(?MODULE, ?CHILD(Type, I, Args)).

start_child(Type, ID, I, Args) -> 
   supervisor:start_child(?MODULE, ?CHILD(Type, ID, I, Args)).

