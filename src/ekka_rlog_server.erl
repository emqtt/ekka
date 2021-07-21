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

%% @doc This module accepts watch and bootstrap requests, and spawns
%% workers processes.

-module(ekka_rlog_server).

-behaviour(gen_server).

%% API
-export([ start_link/2
        , subscribe/3
        , bootstrap_me/2
        , probe/2
        ]).

%% gen_server callbacks
-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , handle_continue/2
        , code_change/3
        ]).

%% Internal exports
-export([do_bootstrap/2, do_probe/1]).

-export_type([checkpoint/0]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("ekka_rlog.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type checkpoint() :: integer().

-record(s,
        { shard               :: ekka_rlog:shard()
        , agent_sup           :: pid()
        , bootstrapper_sup    :: pid()
        , tlog_replay         :: integer()
        , bootstrap_threshold :: integer()
        }).

%%================================================================================
%% API funcions
%%================================================================================

-spec start_link(pid(), ekka_rlog:shard()) -> {ok, pid()}.
start_link(Parent, Shard) ->
    gen_server:start_link({local, Shard}, ?MODULE, {Parent, Shard}, []).

%% @doc Make a call to the server that does nothing.
%%
%% This API function is called by the replicant before `subscribe/3'
%% to reduce the risk of double subscription when the reply from the
%% server is lost or delayed due to network congestion.
-spec probe(node(), ekka_rlog:shard()) -> boolean().
probe(Node, Shard) ->
    ekka_rlog_lib:rpc_call(Node, ?MODULE, do_probe, [Shard]) =:= true.

-spec subscribe(ekka_rlog:shard(), ekka_rlog_lib:subscriber(), checkpoint()) ->
          {ok, _NeedBootstrap :: boolean(), _Agent :: pid(), [ekka_mnesia:table()]}.
subscribe(Shard, Subscriber, Checkpoint) ->
    gen_server:call(Shard, {subscribe, Subscriber, Checkpoint}, infinity).

-spec bootstrap_me(node(), ekka_rlog:shard()) -> {ok, pid()}
                                               | {error, term()}.
bootstrap_me(RemoteNode, Shard) ->
    Me = {node(), self()},
    case ekka_rlog_lib:rpc_call(RemoteNode, ?MODULE, do_bootstrap, [Shard, Me]) of
        {ok, Pid} -> {ok, Pid};
        Err       -> {error, Err}
    end.

%%================================================================================
%% gen_server callbacks
%%================================================================================

init({Parent, Shard}) ->
    logger:set_process_metadata(#{ domain => [ekka, rlog, server]
                                 , shard => Shard
                                 }),
    ?tp(rlog_server_start, #{node => node()}),
    {ok, {Parent, Shard}, {continue, post_init}}.

handle_info({'DOWN', _MRef, process, Pid, _Info}, St) ->
    ekka_rlog_status:notify_agent_disconnect(Pid),
    {noreply, St};
handle_info({mnesia_table_event, Event}, St) ->
    #s{shard = Shard} = St,
    case Event of
        {write, #?schema{mnesia_table = Tab, shard = Shard}, _ActivityId} ->
            ?tp(notice, "Shard schema change",
                #{ shard     => Shard
                 , new_table => Tab
                 }),
            ekka_rlog_sup:restart_shard(Shard, schema_change);
        _ ->
            ok
    end,
    {noreply, St};
handle_info(_Info, St) ->
    {noreply, St}.

handle_continue(post_init, {Parent, Shard}) ->
    ok = ekka_rlog_tab:ensure_table(Shard),
    Tables = process_schema(Shard),
    ekka_rlog_config:load_shard_config(Shard, Tables),
    AgentSup = ekka_rlog_shard_sup:start_agent_sup(Parent, Shard),
    BootstrapperSup = ekka_rlog_shard_sup:start_bootstrapper_sup(Parent, Shard),
    mnesia:wait_for_tables([Shard|Tables], 100000),
    ?tp(notice, "Shard fully up",
        #{ node  => node()
         , shard => Shard
         }),
    State = #s{ shard               = Shard
              , agent_sup           = AgentSup
              , bootstrapper_sup    = BootstrapperSup
              , tlog_replay         = 30 %% TODO: unused. Remove?
              , bootstrap_threshold = 3000 %% TODO: unused. Remove?
              },
    {noreply, State}.

handle_cast(_Cast, St) ->
    {noreply, St}.

handle_call({subscribe, Subscriber, Checkpoint}, _From, State) ->
    #s{ bootstrap_threshold = BootstrapThreshold
      , tlog_replay         = TlogReplay
      , shard               = Shard
      , agent_sup           = AgentSup
      } = State,
    {NeedBootstrap, ReplaySince} = needs_bootstrap( BootstrapThreshold
                                                  , TlogReplay
                                                  , Checkpoint
                                                  ),
    Pid = maybe_start_child(AgentSup, [Subscriber, ReplaySince]),
    monitor(process, Pid),
    ekka_rlog_status:notify_agent_connect(Shard, ekka_rlog_lib:subscriber_node(Subscriber), Pid),
    Tables = ekka_rlog_schema:tables_of_shard(Shard),
    {reply, {ok, NeedBootstrap, Pid, Tables}, State};
handle_call({bootstrap, Subscriber}, _From, State) ->
    Pid = maybe_start_child(State#s.bootstrapper_sup, [Subscriber]),
    {reply, {ok, Pid}, State};
handle_call(probe, _From, State) ->
    {reply, true, State};
handle_call(_From, Call, St) ->
    {reply, {error, {unknown_call, Call}}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

terminate(_Reason, #{} = St) ->
    {ok, St}.

%%================================================================================
%% Internal functions
%%================================================================================

%% @private Check if the remote needs to bootstrap itself
-spec needs_bootstrap(integer(), integer(), checkpoint()) -> {boolean(), integer()}.
%% TODO: TMP workaround, always bootstrap
needs_bootstrap(_, Replay, _) ->
    {true, Replay}.

%% needs_bootstrap(BootstrapThreshold, Replay, Checkpoint) ->
%%     {BootstrapDeadline, _} = ekka_rlog_lib:make_key_in_past(BootstrapThreshold),
%%     case Checkpoint of
%%         {TS, _Node} when TS > BootstrapDeadline ->
%%             {false, TS - Replay};
%%         _ ->
%%             {ReplaySince, _} = ekka_rlog_lib:make_key_in_past(Replay),
%%             {true, ReplaySince}
%%     end.

-spec maybe_start_child(pid(), list()) -> pid().
maybe_start_child(Supervisor, Args) ->
    case supervisor:start_child(Supervisor, Args) of
        {ok, Pid} -> Pid;
        {ok, Pid, _} -> Pid;
        {error, {already_started, Pid}} -> Pid
    end.

-spec process_schema(ekka_rlog:shard()) -> [ekka_mnesia:table()].
process_schema(Shard) ->
    ok = mnesia:wait_for_tables([?schema], infinity),
    mnesia:subscribe({table, ?schema, simple}),
    Tables = ekka_rlog_schema:tables_of_shard(Shard),
    Tables.

%%================================================================================
%% Internal exports (gen_rpc)
%%================================================================================

-spec do_bootstrap(ekka_rlog:shard(), ekka_rlog_lib:subscriber()) -> {ok, pid()}.
do_bootstrap(Shard, Subscriber) ->
    gen_server:call(Shard, {bootstrap, Subscriber}, infinity).

-spec do_probe(ekka_rlog:shard()) -> true.
do_probe(Shard) ->
    gen_server:call(Shard, probe, 1000).
