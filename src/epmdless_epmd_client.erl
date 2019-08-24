-module(epmdless_epmd_client).

%% epmd_module callbacks
-export([start_link/0,
         register_node/2,
         register_node/3,
         port_please/2,
         names/1]).

%% The supervisor module erl_distribution tries to add us as a child
%% process.  We don't need a child process, so return 'ignore'.
start_link() ->
    % inets:start(),
    ignore.

register_node(Name, Port) ->
    io:format("====> ~p ~p~n", [Name, Port]),
    %% This is where we would connect to epmd and tell it which port
    %% we're listening on, but since we're epmd-less, we don't do that.

    Node = erlang:iolist_to_binary(erlang:atom_to_list(Name)),
    Pckt = erlang:iolist_to_binary(erlang:integer_to_list(Port)),

    {ok, Sock} = gen_tcp:connect({127,0,0,1}, 4370, [{active, false}, binary, {packet, http}], 10000),
    gen_tcp:send(Sock, <<"PUT /nodes/", Node/binary, " HTTP/1.1\r\nConnection: close\r\nContent-Length:4\r\n\r\n", Pckt/binary>>),
    {ok, {http_response, {1,1}, 200, _}} = gen_tcp:recv(Sock, 0),
    gen_tcp:close(Sock),


    % TODO: raw protocol
    % inets:start(),
    % Url = io_lib:format("http://localhost:4370/nodes/~s", [Name]),
    % {ok, {{_, 200, _}, _, _}} = httpc:request(put, 
    %     {Url, [], "text/plain", erlang:integer_to_list(Port)}, [], []
    % ),

    %% Need to return a "creation" number between 1 and 3.
    Creation = rand:uniform(3),
    {ok, Creation}.

%% As of Erlang/OTP 19.1, register_node/3 is used instead of
%% register_node/2, passing along the address family, 'inet_tcp' or
%% 'inet6_tcp'.  This makes no difference for our purposes.
register_node(Name, Port, _Family) ->
    register_node(Name, Port).

port_please(Name, IP) ->

    %% @todo: use raw http client



    io:format("====> ~p ~p~n", [Name, inet:ntoa(IP)]),
    {ok, {{_, 200, _}, _, Port}} = httpc:request(io_lib:format("http://~s:4370/nodes/~s", [inet:ntoa(IP), Name])),
    io:format("====> ~p~n", [Port]),

    % Port = epmdless:dist_port(Name),
    % Port = 32791,
    %% The distribution protocol version number has been 5 ever since
    %% Erlang/OTP R6.
    Version = 5,
    {port, erlang:list_to_integer(Port), Version}.

names(Hostname) ->
    io:format("=[  ]===> ~p ~p~n", [Hostname]),
    %% Since we don't have epmd, we don't really know what other nodes
    %% there are.
    {error, address}.
