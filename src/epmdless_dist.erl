-module(epmdless_dist).

-export([listen/1,
         select/1,
         accept/1,
         accept_connection/5,
         setup/5,
         close/1,
         childspecs/0]).

listen(Name) ->
    %% Here we figure out what port we want to listen on.

    Port = epmdless:dist_port(Name),

    %% Set both "min" and "max" variables, to force the port number to
    %% this one.
    ok = application:set_env(kernel, inet_dist_listen_min, Port),
    ok = application:set_env(kernel, inet_dist_listen_max, Port),

    %% Finally run the real function!
    inet_tcp_dist:listen(Name).

select(Node) ->
    inet_tcp_dist:select(Node).

accept(Listen) ->
    inet_tcp_dist:accept(Listen).

accept_connection(AcceptPid, Socket, MyNode, Allowed, SetupTime) ->
    inet_tcp_dist:accept_connection(AcceptPid, Socket, MyNode, Allowed, SetupTime).

setup(Node, Type, MyNode, LongOrShortNames, SetupTime) ->
    inet_tcp_dist:setup(Node, Type, MyNode, LongOrShortNames, SetupTime).

close(Listen) ->
    inet_tcp_dist:close(Listen).

childspecs() ->
    inet_tcp_dist:childspecs().
