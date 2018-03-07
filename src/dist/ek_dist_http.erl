%% @doc
%%
-module(ek_dist_http).
-compile({parse_transform, category}).


-export([
   get/2,
   put/2
]).

-define(PORT,    4370).
-define(SO,      [{active, false}, binary, {packet, http}]).
-define(TIMEOUT, 10000).

get(Host, Path) ->
   [either ||
      Sock <- gen_tcp:connect(Host, ?PORT, ?SO, ?TIMEOUT),
      Req  =< <<"GET", $ , Path/binary, " HTTP/1.1\r\nConnection: close\r\n\r\n">>,
      gen_tcp:send(Sock, Req),
      recv_http_ok(Sock),
      recv_http_head(Sock),
      Ret  <- cats:unit(gen_tcp:recv(Sock, 0)),
      gen_tcp:close(Sock),
      cats:flatten(Ret)
   ].

recv_http_ok(Sock) ->
   case gen_tcp:recv(Sock, 0) of
      {ok, {http_response, {1,1}, 200, _}} ->
         ok;
      _ ->
         {error, badarg}
   end.

recv_http_head(Sock) ->
   case gen_tcp:recv(Sock, 0) of
      {ok, http_eoh} ->
         inet:setopts(Sock, [{packet, raw}]),
         ok;
      {ok, X} ->
         recv_http_head(Sock);
      _ ->
         {error, badarg}
   end.


put(Path, Pckt) ->
   [either ||
      Sock <- gen_tcp:connect({127,0,0,1}, 4370, [{active, false}, binary, {packet, http}], 5000),
      Len  =< scalar:s(byte_size(Pckt)),
      Req  =< <<"PUT", $ , Path/binary, " HTTP/1.1\r\nConnection: close\r\nContent-Length: ", Len/binary, "\r\n\r\n", Pckt/binary>>,
      gen_tcp:send(Sock, Req),
      Ret  <- cats:unit(gen_tcp:recv(Sock, 0)),
      gen_tcp:close(Sock),
      cats:flatten(Ret)
   ].



    % {ok, Sock} = gen_tcp:connect({127,0,0,1}, 4370, [{active, false}, binary, {packet, http}], 10000),
    % gen_tcp:send(Sock, <<"PUT /nodes/", Node/binary, " HTTP/1.1\r\nConnection: close\r\nContent-Length:4\r\n\r\n", Pckt/binary>>),
    % {ok, {http_response, {1,1}, 200, _}} = gen_tcp:recv(Sock, 0),
    % gen_tcp:close(Sock),   