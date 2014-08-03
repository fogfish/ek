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
% -define(CONFIG_DEBUG, true).

%% default seed timeout
-define(CONFIG_SEED_INTERVAL,  60000).

%%
%% default gossip configuration
-define(CONFIG_GOSSIP_TIMEOUT,  10000).
-define(CONFIG_GOSSIP_EXCHANGE,     3).


-ifdef(CONFIG_DEBUG).
   -define(DEBUG(Str, Args), error_logger:error_msg(Str, Args)).
-else.
   -define(DEBUG(Str, Args), ok).
-endif.
