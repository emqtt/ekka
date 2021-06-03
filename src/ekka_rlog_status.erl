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

%% @doc This module holds status of the RLOG replicas and manages
%% event subscribers.
-module(ekka_rlog_status).

-behaviour(gen_event).

%% API:
-export([start_link/0, subscribe_events/0, unsubscribe_events/1, notify_shard_up/2,
         notify_shard_down/1, wait_for_shards/2, upstream/1, upstream_node/1,
         shards_up/0, shards_down/0, get_shard_stats/1,

         notify_core_node_up/2, notify_core_node_down/1, get_core_node/2,

         notify_replicant_state/2, notify_replicant_import_trans/2,
         notify_replicant_replayq_len/2,
         notify_replicant_bootstrap_start/1, notify_replicant_bootstrap_complete/1,
         notify_replicant_bootstrap_import/1
        ]).

%% gen_event callbacks:
-export([init/1, handle_call/2, handle_event/2]).

-define(SERVER, ?MODULE).

-record(s,
        { ref        :: reference()
        , subscriber :: pid()
        }).

-include("ekka_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% Tables and table keys:
-define(replica_tab, ekka_rlog_replica_tab).
-define(upstream_pid, upstream_pid).
-define(core_node, core_node).

-define(stats_tab, ekka_rlog_stats_tab).
-define(replicant_state, replicant_state).
-define(replicant_import, replicant_import).
-define(replicant_replayq_len, replicant_replayq_len).
-define(replicant_bootstrap_start, replicant_bootstrap_start).
-define(replicant_bootstrap_complete, replicant_bootstrap_complete).
-define(replicant_bootstrap_import, replicant_bootstrap_import).

%%================================================================================
%% API funcions
%%================================================================================

%% @doc Return core node used as the upstream for the replica
-spec upstream_node(ekka_rlog:shard()) -> {ok, node()} | disconnected.
upstream_node(Shard) ->
    case upstream(Shard) of
        {ok, Pid}    -> {ok, node(Pid)};
        disconnected -> disconnected
    end.

%% @doc Return pid of the core node agent that serves us.
-spec upstream(ekka_rlog:shard()) -> {ok, pid()} | disconnected.
upstream(Shard) ->
    case ets:lookup(?replica_tab, {?upstream_pid, Shard}) of
        [{_, Node}] -> {ok, Node};
        []          -> disconnected
    end.

-spec start_link() -> {ok, pid()}.
start_link() ->
    %% Create a table that holds state of the replicas:
    ets:new(?replica_tab, [ ordered_set
                          , named_table
                          , public
                          , {write_concurrency, false}
                          , {read_concurrency, true}
                          ]),
    ets:new(?stats_tab, [ set
                        , named_table
                        , public
                        , {write_concurrency, true}
                        ]),
    gen_event:start_link({local, ?SERVER}, []).

-spec notify_shard_up(ekka_rlog:shard(), _AgentPid :: pid()) -> ok.
notify_shard_up(Shard, Upstream) ->
    do_notify_up(?upstream_pid, Shard, Upstream).

-spec notify_shard_down(ekka_rlog:shard()) -> ok.
notify_shard_down(Shard) ->
    do_notify_down(?upstream_pid, Shard),
    %% Delete metrics
    ets:insert(?stats_tab, {{?replicant_state, Shard}, down}),
    lists:foreach(fun(Key) -> ets:delete(?stats_tab, {Key, Shard}) end,
                  [?replicant_import,
                   ?replicant_replayq_len,
                   ?replicant_bootstrap_start,
                   ?replicant_bootstrap_complete,
                   ?replicant_bootstrap_import
                  ]).

-spec notify_core_node_up(ekka_rlog:shard(), node()) -> ok.
notify_core_node_up(Shard, Node) ->
    do_notify_up(?core_node, Shard, Node).

-spec notify_core_node_down(ekka_rlog:shard()) -> ok.
notify_core_node_down(Shard) ->
    do_notify_down(?core_node, Shard).

%% Get a healthy core node that has the specified shard, and can
%% accept or RPC calls.
-spec get_core_node(ekka_rlog:shard(), timeout()) -> {ok, node()} | timeout.
get_core_node(Shard, Timeout) ->
    case ets:lookup(?replica_tab, {?core_node, Shard}) of
        [{_, Node}] ->
            {ok, Node};
        [] ->
            case wait_objects(?core_node, [Shard], Timeout) of
                ok           -> get_core_node(Shard, 0);
                {timeout, _} -> timeout
            end
    end.

-spec subscribe_events() -> reference().
subscribe_events() ->
    Self = self(),
    Ref = monitor(process, ?SERVER),
    ok = gen_event:add_sup_handler(?SERVER, {?MODULE, Ref}, [Ref, Self]),
    Ref.

-spec unsubscribe_events(reference()) -> ok.
unsubscribe_events(Ref) ->
    ok = gen_event:delete_handler(?SERVER, {?MODULE, Ref}, []),
    demonitor(Ref, [flush]),
    flush_events(Ref).

-spec wait_for_shards([ekka_rlog:shard()], timeout()) -> ok | {timeout, [ekka_rlog:shard()]}.
wait_for_shards(Shards, Timeout) ->
    ?tp(notice, "Waiting for shards",
        #{ shards => Shards
         , timeout => Timeout
         }),
    Ret = wait_objects(?upstream_pid, Shards, Timeout),
    ?tp(notice, "Done waiting for shards",
        #{ shards => Shards
         , result =>  Ret
         }),
    Ret.

-spec shards_up() -> [ekka_rlog:shard()].
shards_up() ->
    objects_up(?upstream_pid).

-spec objects_up(atom()) -> [term()].
objects_up(Tag) ->
    lists:append(ets:match(?replica_tab, {{Tag, '$1'}, '_'})).

-spec shards_down() -> [ekka_rlog:shard()].
shards_down() ->
    ekka_rlog_config:shards() -- shards_up().

-spec get_shard_stats(ekka_rlog:shard()) -> map().
get_shard_stats(Shard) ->
    case ekka_rlog:role() of
        core ->
            #{}; %% TODO
        replicant ->
            case upstream_node(Shard) of
                {ok, Upstream} -> ok;
                _ -> Upstream = undefined
            end,
            #{ state               => get_stat(Shard, ?replicant_state)
             , last_imported_trans => get_stat(Shard, ?replicant_import)
             , replayq_len         => get_stat(Shard, ?replicant_replayq_len)
             , bootstrap_time      => get_bootstrap_time(Shard)
             , bootstrap_num_keys  => get_stat(Shard, ?replicant_bootstrap_import)
             , upstream            => Upstream
             }
    end.

%% Note on the implementation: `rlog_replicant' and `rlog_agent'
%% processes may have long message queues, esp. during bootstrap.

-spec notify_replicant_state(ekka_rlog:shard(), atom()) -> ok.
notify_replicant_state(Shard, State) ->
    set_stat(Shard, ?replicant_state, State).

-spec notify_replicant_import_trans(ekka_rlog:shard(), ekka_rlog_server:checkpoint()) -> ok.
notify_replicant_import_trans(Shard, Checkpoint) ->
    set_stat(Shard, ?replicant_import, Checkpoint).

-spec notify_replicant_replayq_len(ekka_rlog:shard(), integer()) -> ok.
notify_replicant_replayq_len(Shard, N) ->
    set_stat(Shard, ?replicant_replayq_len, N).

-spec notify_replicant_bootstrap_start(ekka_rlog:shard()) -> ok.
notify_replicant_bootstrap_start(Shard) ->
    set_stat(Shard, ?replicant_bootstrap_start, os:system_time(millisecond)).

-spec notify_replicant_bootstrap_complete(ekka_rlog:shard()) -> ok.
notify_replicant_bootstrap_complete(Shard) ->
    set_stat(Shard, ?replicant_bootstrap_complete, os:system_time(millisecond)).

-spec notify_replicant_bootstrap_import(ekka_rlog:shard()) -> ok.
notify_replicant_bootstrap_import(Shard) ->
    Key = {?replicant_bootstrap_import, Shard},
    Op = {2, 1},
    ets:update_counter(?stats_tab, Key, Op, {Key, 0}),
    ok.

%%================================================================================
%% gen_event callbacks
%%================================================================================

init([Ref, Subscriber]) ->
    logger:set_process_metadata(#{domain => [ekka, rlog, event_mgr]}),
    ?tp(start_event_monitor,
        #{ reference  => Ref
         , subscriber => Subscriber
         }),
    State = #s{ ref        = Ref
              , subscriber = Subscriber
              },
    {ok, State, hibernate}.

handle_call(_, State) ->
    {ok, {error, unknown_call}, State, hibernate}.

handle_event(Event, State = #s{ref = Ref, subscriber = Sub}) ->
    Sub ! {Ref, Event},
    {ok, State, hibernate}.

%%================================================================================
%% Internal functions
%%================================================================================

-spec wait_objects(atom(), [A], timeout()) -> ok | {timeout, [A]}.
wait_objects(Tag, Objects, Timeout) ->
    ERef = subscribe_events(),
    TRef = ekka_rlog_lib:send_after(Timeout, self(), {ERef, timeout}),
    %% Exclude shards that are up, since they are not going to send any events:
    DownObjects = Objects -- objects_up(Tag),
    Ret = do_wait_objects(Tag, ERef, DownObjects),
    ekka_rlog_lib:cancel_timer(TRef),
    unsubscribe_events(ERef),
    Ret.

do_wait_objects(_, _, []) ->
    ok;
do_wait_objects(Tag, ERef, RemainingObjects) ->
    receive
        {'DOWN', ERef, _, _, _} ->
            error(rlog_restarted);
        {ERef, timeout} ->
            {timeout, RemainingObjects};
        {ERef, {{Tag, Object}, _Value}} ->
            do_wait_objects(Tag, ERef, RemainingObjects -- [Object])
    end.

flush_events(ERef) ->
    receive
        {gen_event_EXIT, {?MODULE, ERef}, _} ->
            flush_events(ERef);
        {ERef, _} ->
            flush_events(ERef)
    after 0 ->
            ok
    end.

-spec set_stat(ekka_rlog:shard(), atom(), term()) -> ok.
set_stat(Shard, Stat, Val) ->
    ets:insert(?stats_tab, {{Stat, Shard}, Val}),
    ok.

-spec get_stat(ekka_rlog:shard(), atom()) -> term() | undefined.
get_stat(Shard, Stat) ->
    case ets:lookup(?stats_tab, {Stat, Shard}) of
        [{_, Val}] -> Val;
        []         -> undefined
    end.

-spec get_bootstrap_time(ekka_rlog:shard()) -> integer() | undefined.
get_bootstrap_time(Shard) ->
    case {get_stat(Shard, ?replicant_bootstrap_start), get_stat(Shard, ?replicant_bootstrap_complete)} of
        {undefined, undefined} ->
            undefined;
        {Start, undefined} ->
            os:system_time(millisecond) - Start;
        {Start, Complete} ->
            Complete - Start
    end.

-spec do_notify_up(atom(), term(), term()) -> ok.
do_notify_up(Tag, Object, Value) ->
    Key = {Tag, Object},
    New = not ets:member(?replica_tab, Key),
    ets:insert(?replica_tab, {Key, Value}),
    case New of
        true ->
            ?tp(ekka_rlog_status_change,
                #{ status => up
                 , tag    => Tag
                 , key    => Object
                 , value  => Value
                 , node   => node()
                 }),
            gen_event:notify(?SERVER, {Key, Value});
        false ->
            ok
    end.

-spec do_notify_down(atom(), term()) -> ok.
do_notify_down(Tag, Object) ->
    Key = {Tag, Object},
    ets:delete(?replica_tab, Key),
    ?tp(ekka_rlog_status_change,
        #{ status => down
         , key    => Object
         , tag    => Tag
         }).
