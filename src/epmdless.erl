-module(epmdless).

-export([dist_port/1]).

%% Return the port number to be used by a certain node.
dist_port(Name) when is_atom(Name) ->
   dist_port(atom_to_list(Name));
dist_port(Name) when is_list(Name) ->
   %% Figure out the base port.  If not specified using the
   %% inet_dist_base_port kernel environment variable, default to
   %% 4370, one above the epmd port.
   BasePort = application:get_env(kernel, inet_dist_base_port, 4370),

   %% Now, figure out our "offset" on top of the base port.  The
   %% offset is the integer just to the left of the @ sign in our node
   %% name.  If there is no such number, the offset is 0.
   %%
   %% Also handle the case when no hostname was specified.
   NodeName = re:replace(Name, "@.*$", ""),
   Offset =
     case re:run(NodeName, "[0-9]+$", [{capture, first, list}]) of
         nomatch ->
             0;
         {match, [OffsetAsString]} ->
             list_to_integer(OffsetAsString)
     end,

   BasePort + Offset.