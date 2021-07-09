%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% Top level supervisor for the RLOG tree, that starts the persistent
%% processes.
-module(ekka_rlog_top_sup).

-behaviour(supervisor).

-export([init/1, start_link/0]).

-define(SUPERVISOR, ?MODULE).

-include("ekka_rlog.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%================================================================================
%% API funcions
%%================================================================================

start_link() ->
    Role = ekka_rlog:role(),
    supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, Role).

%%================================================================================
%% supervisor callbacks
%%================================================================================

init(core) ->
    SupFlags = #{ strategy => one_for_all
                , intensity => 1
                , period => 1
                },
    Children = [status_mgr(), child_sup()],
    {ok, {SupFlags, Children}};
init(replicant) ->
    SupFlags = #{ strategy => one_for_all
                , intensity => 1
                , period => 1
                },
    Children = [status_mgr(), core_node_lb(), child_sup()],
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal functions
%%================================================================================

status_mgr() ->
    #{ id => ekka_rlog_status
     , start => {ekka_rlog_status, start_link, []}
     , restart => permanent
     , shutdown => 5000
     , type => worker
     }.

core_node_lb() ->
    #{ id => ekka_rlog_lb
     , start => {ekka_rlog_lb, start_link, []}
     , restart => permanent
     , shutdown => 5000
     , type => worker
     }.

child_sup() ->
    #{ id => ekka_rlog_sup
     , start => {ekka_rlog_sup, start_link, []}
     , restart => permanent
     , shutdown => infinity
     , type => supervisor
     }.
