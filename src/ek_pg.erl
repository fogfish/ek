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
%%   distributed process group topology that observes changes and notifies 
%%   local members
-module(ek_pg).
-behaviour(pipe).
-include("ek.hrl").
-compile({parse_transform, category}).

-export([
   start_link/2,
   init/1,
   free/2,
   handle/3
]).

%%
%%
-record(state, {
   name      = undefined :: atom(),       %% process group name
   peers     = undefined :: _,            %% remote peers
   watchdogs = undefined :: _,            %% local process watchdogs
   processes = undefined :: crdts:orset() %% local process dictionary

}).

%%%------------------------------------------------------------------
%%%
%%% factory
%%%
%%%------------------------------------------------------------------   

start_link(Name, _Opts) ->
   pipe:start_link({local, Name}, ?MODULE, [Name], []).

init([Name]) ->
   Node = erlang:node(),
   [erlang:send({Name, Peer}, {peerup, Node}) || 
      Peer <- erlang:nodes()], 
   _ = net_kernel:monitor_nodes(true),
   {ok, handle,
      #state{
         name      = Name,
         peers     = bst:new(),
         watchdogs = bst:new(),
         processes = crdts_orset:new()
      }
   }.


free(_Reason, _State) ->
   ok.


%%%------------------------------------------------------------------
%%%
%%% state machine
%%%
%%%------------------------------------------------------------------   

%%
%% peer managements
%%
handle({peerup, Node}, _, State) ->
   {next_state, handle, peerup(Node, State)};

handle({nodeup, Node}, _, #state{name = Name} = State) ->
   ?DEBUG("ek: pg ~s nodeup ~s~n", [Name, Node]),
   erlang:send({Name, Node}, {peerup, erlang:node()}),
   {next_state, handle, State};

handle({'DOWN', _Ref, process, {Name, Node}, _Reason}, _, #state{name = Name} = State) ->
   {next_state, handle, peerdown(Node, State)};

handle({nodedown, Node}, _, State) ->
   {next_state, handle, peerdown(Node, State)};

handle({reconcile, Remote}, _, State) ->
   {next_state, handle, reconcile(Remote, State)};

%%
%% process management
%%
handle({join, Addr, Pid}, Pipe, State0) ->
   State1 = join(Addr, Pid, State0),
   pipe:ack(Pipe, ok),
   {next_state, handle, State1};

handle({leave, Pid}, Pipe, State0) ->
   State1 = leave(Pid, State0),
   pipe:ack(Pipe, ok),
   {next_state, handle, State1};

handle(size, Pipe, #state{processes = Pids} = State) ->
   pipe:ack(Pipe, length(crdts_orset:value(Pids))),
   {next_state, handle, State};

handle(members, Pipe, #state{processes = Pids} = State) ->
   pipe:ack(Pipe, [Pid || {Pid, _Addr} <- crdts_orset:value(Pids)]),
   {next_state, handle, State};

handle(address, Pipe, #state{processes = Pids} = State) ->
   pipe:ack(Pipe, [Addr || {_Pid, Addr} <- crdts_orset:value(Pids)]),
   {next_state, handle, State};

handle(peers, Pipe, #state{peers = Peers} = State) ->
   pipe:ack(Pipe, bst:keys(Peers)),
   {next_state, handle, State};

handle({'DOWN', _Ref, process, Pid, _Reason}, _, State) ->
   {next_state, handle, leave(Pid, State)}.


%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   

%%
%%
peerup(Node, #state{name = Name, peers = Peers, processes = Pids} = State) ->
   case bst:lookup(Node, Peers) of
      undefined ->
         ?DEBUG("ek: pg ~s peerup ~s~n", [Name, Node]),
         Ref = erlang:monitor(process, {Name, Node}),
         erlang:send({Name, Node}, {peerup, erlang:node()}),
         erlang:send({Name, Node}, {reconcile, Pids}),
         State#state{peers = bst:insert(Node, Ref, Peers)};
      _ ->
         State
   end.


%%
%%
peerdown(Node, #state{name = Name, peers = Peers, processes = Pids0} = State) ->
   case bst:lookup(Node, Peers) of
      undefined ->
         State;
      Ref ->
         ?DEBUG("ek: pg ~s peerdown ~s~n", [Name, Node]),
         erlang:demonitor(Ref, [flush]),
         Pids1 = crdts_orset:filter(
            fun({Pid, Addr}) ->
               case erlang:node(Pid) of
                  Node ->
                     send_to_local({leave, Pid, Addr}, State),
                     false;
                  _ ->
                     true
               end
            end,
            Pids0
         ),
         State#state{peers = bst:remove(Node, Peers), processes = Pids1}
   end.

%%
%%
reconcile(#state{name = Name, peers = Peers, processes = Pids}) ->
   bst:foreach(
      fun({Node, _}) ->
         erlang:send({Name, Node}, {reconcile, Pids})
      end,
      Peers
   ).

reconcile(Remote, #state{name = Name, processes = Local} = State) ->
   Sub = diff(Local, Remote), 
   ?DEBUG("ek: pg ~s [-] ~p~n", [Name, Sub]),
   lists:foreach(
      fun(Pid) ->
         Addr = orddict:fetch(Pid, crdts_orset:value(Local)),
         send_to_local({leave, Addr, Pid}, State)
      end,
      Sub
   ),

   Add = diff(Remote, Local),
   ?DEBUG("ek: pg ~s [+] ~p~n", [Name, Add]),
   lists:foreach(
      fun(Pid) ->
         Addr = orddict:fetch(Pid, crdts_orset:value(Remote)),
         send_to_local({join, Addr, Pid}, State)
      end,
      Add
   ),

   State#state{processes = crdts_orset:join(Remote, Local)}.


%%
%%
join(Addr, Pid, #state{watchdogs = Refs0, processes = Pids0} = State0) ->
   Refs1  = bst:insert(Pid, erlang:monitor(process, Pid), Refs0),
   Pids1  = crdts_orset:insert({Pid, Addr}, Pids0),
   State1 = State0#state{watchdogs = Refs1, processes = Pids1},
   send_to_local({join, Addr, Pid}, State0),
   reconcile(State1),
   State1.

%%
%%
leave(Pid, #state{watchdogs = Refs0, processes = Pids0} = State0) ->
   Addr   = orddict:fetch(Pid, crdts_orset:value(Pids0)),
   Ref    = bst:lookup(Pid, Refs0),
    _     = erlang:demonitor(Ref, [flush]),
   Refs1  = bst:remove(Pid, Refs0),   
   Pids1  = crdts_orset:remove({Pid, Addr}, Pids0),
   State1 = State0#state{watchdogs = Refs1, processes = Pids1},
   send_to_local({leave, Addr, Pid}, State1),
   reconcile(State1),
   State1.


%%
%%
send_to_local(Msg, #state{processes = Pids} = State) ->
   lists:foreach(
      fun({Pid, _Addr}) ->
         case erlang:node(Pid) of
            Node when Node =:= erlang:node() ->
               Pid ! Msg;
            _ ->
               ok
         end
      end,
      crdts_orset:value(Pids)
   ).

%%
%% Returns only the elements of SetA that are not also elements of SetB.
diff(OrSetA, OrSetB) ->
   A = [Pid || 
      {Pid, _Addr} <- crdts_orset:value(OrSetA), 
      erlang:node(Pid) =/= erlang:node()],

   B = [Pid || 
      {Pid, _Addr} <- crdts_orset:value(OrSetB), 
      erlang:node(Pid) =/= erlang:node()],

   gb_sets:to_list(
      gb_sets:difference(
         gb_sets:from_list(A),
         gb_sets:from_list(B)
      )
   ).


