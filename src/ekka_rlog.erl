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

%% API and management functions for asynchronous Mnesia replication
-module(ekka_rlog).

-export([ init/0

        , status/0

        , role/0
        , role/1
        , backend/0

        , core_nodes/0
        , subscribe/4
        , wait_for_shards/2
        ]).

-export_type([ shard/0
             , role/0
             , shard_config/0
             ]).

-include("ekka_rlog.hrl").
-include_lib("mnesia/src/mnesia.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-type shard() :: atom().

-type role() :: core | replicant.

-type shard_config() :: #{ tables := [ekka_mnesia:table()]
                         , match_spec := ets:match_spec()
                         }.

init() ->
    ekka_rlog_config:load_config().

status() ->
    Backend = backend(),
    Role    = role(),
    Info0 = #{ backend => Backend
             , role    => Role
             },
    case {Backend, Role} of
        {mnesia, _} ->
            Info0;
        {rlog, replicant} ->
            Stats = [{I, ekka_rlog_status:get_shard_stats(I)}
                     || I <- ekka_rlog_schema:shards()],
            Info0#{ shards_in_sync => ekka_rlog_status:shards_up()
                  , shards_down    => ekka_rlog_status:shards_down()
                  , shard_stats    => maps:from_list(Stats)
                  };
        {rlog, core} ->
            Info0 %% TODO
    end.

-spec role() -> ekka_rlog:role().
role() ->
    ekka_rlog_config:role().

-spec role(node()) -> ekka_rlog:role().
role(Node) ->
    ekka_rlog_lib:rpc_call(Node, ?MODULE, role, []).

backend() ->
    ekka_rlog_config:backend().

-spec core_nodes() -> [node()].
core_nodes() ->
    application:get_env(ekka, core_nodes, []).

-spec wait_for_shards([shard()], timeout()) -> ok | {timeout, [shard()]}.
wait_for_shards(Shards, Timeout) ->
    case ekka_rlog_config:backend() of
        rlog ->
            lists:foreach(fun ensure_shard/1, Shards),
            case role() of
                core ->
                    ok;
                replicant ->
                    ekka_rlog_status:wait_for_shards(Shards, Timeout)
            end;
        mnesia ->
            ok
    end.

-spec ensure_shard(shard()) -> ok.
ensure_shard(Shard) ->
    case ekka_rlog_sup:start_shard(Shard) of
        {ok, _}                       -> ok;
        {error, already_present}      -> ok;
        {error, {already_started, _}} -> ok;
        Err                           -> error({failed_to_create_shard, Shard, Err})
    end.

-spec subscribe(ekka_rlog:shard(), node(), pid(), ekka_rlog_server:checkpoint()) ->
          {ok, _NeedBootstrap :: boolean(), _Agent :: pid(), [ekka_mnesia:table()]}
        | {badrpc | badtcp, term()}.
subscribe(Shard, RemoteNode, Subscriber, Checkpoint) ->
    case ekka_rlog_server:probe(RemoteNode, Shard) of
        true ->
            MyNode = node(),
            Args = [Shard, {MyNode, Subscriber}, Checkpoint],
            ekka_rlog_lib:rpc_call(RemoteNode, ekka_rlog_server, subscribe, Args);
        false ->
            {badrpc, probe_failed}
    end.
