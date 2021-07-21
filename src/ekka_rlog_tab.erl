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

%% access module for transaction logs
%% implemented as a ram_copies mnesia table
-module(ekka_rlog_tab).

%% Mnesia bootstrap
-export([mnesia/1, ensure_table/1]).

-export([write/3]).

-include("ekka_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-type key() :: ekka_rlog_lib:txid().
-type shard() :: ekka_rlog:shard().

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% @doc Mnesia bootstrap.
mnesia(_BootType) ->
    {ok, _} = ekka_mnesia_null_storage:register().

%% @doc Create or copy shard table
ensure_table(Shard) ->
    Opts = [ {type, ordered_set}
           , {record_name, rlog}
           , {attributes, record_info(fields, rlog)}
           , {null_copies, [node()]}
           ],
    ?tp(notice, creating_rlog_tab,
        #{ node => node()
         , shard => Shard
         }),
    ok = ekka_mnesia:create_table_internal(Shard, Opts),
    ok = ekka_mnesia:copy_table(Shard, null_copies).

%% @doc Write a transaction log.
-spec write(ekka_rlog:shard(), ekka_rlog_lib:txid(), [ekka_rlog_lib:op(),...]) -> ok.
write(Shard, Key, [_ | _] = Ops) ->
    Log = #rlog{ key = Key
               , ops = Ops
               },
    mnesia:write(Shard, Log, write).
