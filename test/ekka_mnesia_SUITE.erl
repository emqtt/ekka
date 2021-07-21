%%--------------------------------------------------------------------
%% Copyright (c) 2019-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ekka_mnesia_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-record(kv_tab, {key, val}).

-define(replica, ?snk_meta := #{domain := [ekka, rlog, replica|_]}).

all() -> ekka_ct:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(TestCase, Config) ->
    ekka_ct:cleanup(TestCase),
    snabbkaffe:stop(),
    Config.

t_data_dir(_) ->
    ekka_mnesia:data_dir().

t_create_del_table(_) ->
    try
        ekka:start(),
        ok = ekka_mnesia:create_table(kv_tab, [
                    {ram_copies, [node()]},
                    {record_name, kv_tab},
                    {attributes, record_info(fields, kv_tab)},
                    {storage_properties, []}]),
        ok = ekka_mnesia:copy_table(kv_tab, disc_copies),
        ok = mnesia:dirty_write(#kv_tab{key = a, val = 1}),
        {atomic, ok} = mnesia:del_table_copy(kv_tab, node())
    after
        application:stop(ekka),
        ekka_mnesia:ensure_stopped()
    end.

%% -spec(join_cluster(node()) -> ok).
%% -spec(leave_cluster(node()) -> ok | {error, any()}).
t_join_leave_cluster(_) ->
    Cluster = ekka_ct:cluster([core, core], []),
    try
        %% Implicitly causes N1 to join N0:
        [N0, N1] = Nodes = ekka_ct:start_cluster(ekka, Cluster),
        ekka_ct:run_on(N0,
          fun() ->
                  #{running_nodes := [N0, N1]} = ekka_mnesia:cluster_info(),
                  [N0, N1] = lists:sort(ekka_mnesia:running_nodes()),
                  ok = rpc:call(N1, ekka_mnesia, leave_cluster, []),
                  #{running_nodes := [N0]} = ekka_mnesia:cluster_info(),
                  [N0] = ekka_mnesia:running_nodes()
          end)
    after
        ok = ekka_ct:teardown_cluster(Cluster)
    end.

%% -spec(cluster_status(node()) -> running | stopped | false).
t_cluster_status(_) ->
    Cluster = ekka_ct:cluster([core, core], []),
    try
        [N0, N1] = ekka_ct:start_cluster(ekka, Cluster),
        running = rpc:call(N0, ekka_mnesia, cluster_status, [N1]),
        running = rpc:call(N1, ekka_mnesia, cluster_status, [N0])
    after
        ok = ekka_ct:teardown_cluster(Cluster)
    end.

%% -spec(remove_from_cluster(node()) -> ok | {error, any()}).
t_remove_from_cluster(_) ->
    Cluster = ekka_ct:cluster([core, core, replicant, replicant], ekka_mnesia_test_util:common_env()),
    try
        [N0, N1, N2, N3] = ekka_ct:start_cluster(ekka, Cluster),
        timer:sleep(1000),
        ekka_ct:run_on(N0, fun() ->
            #{running_nodes := [N0, N1, N2, N3]} = ekka_mnesia:cluster_info(),
            [N0, N1, N2, N3] = lists:sort(ekka_mnesia:running_nodes()),
            [N0, N1, N2, N3] = lists:sort(ekka_mnesia:cluster_nodes(all)),
            [N0, N1, N2, N3] = lists:sort(ekka_mnesia:cluster_nodes(running)),
            [] = ekka_mnesia:cluster_nodes(stopped),
            ok
          end),
        ekka_ct:run_on(N2, fun() ->
            #{running_nodes := [N0, N1, N2, N3]} = ekka_mnesia:cluster_info(),
            [N0, N1, N2, N3] = lists:sort(ekka_mnesia:running_nodes()),
            [N0, N1, N2, N3] = lists:sort(ekka_mnesia:cluster_nodes(all)),
            [N0, N1, N2, N3] = lists:sort(ekka_mnesia:cluster_nodes(running)),
            [] = ekka_mnesia:cluster_nodes(stopped),
            ok
          end),
        ekka_ct:run_on(N0, fun() ->
            ok = ekka_mnesia:remove_from_cluster(N1),
            Running = ekka_mnesia:running_nodes(),
            All = ekka_mnesia:cluster_nodes(all),
            ?assertMatch(false, lists:member(N1, Running)),
            ?assertMatch(false, lists:member(N1, All)),
            ok = rpc:call(N1, ekka_mnesia, ensure_stopped, [])
          end)
    after
        ok = ekka_ct:teardown_cluster(Cluster)
    end.

%% This test runs should walk the replicant state machine through all
%% the stages of startup and online transaction replication, so it can
%% be used to check if anything is _obviously_ broken.
t_rlog_smoke_test(_) ->
    snabbkaffe:fix_ct_logging(),
    Env = [ {ekka, bootstrapper_chunk_config, #{count_limit => 3}}
          | ekka_mnesia_test_util:common_env()
          ],
    Cluster = ekka_ct:cluster([core, core, replicant], Env),
    CounterKey = counter,
    ?check_trace(
       #{timetrap => 10000},
       try
           %% Inject some orderings to make sure the replicant
           %% receives transactions in all states.
           %%
           %% 1. Commit some transactions before the replicant start:
           ?force_ordering(#{?snk_kind := trans_gen_counter_update, value := 5}, #{?snk_kind := state_change, to := disconnected}),
           %% 2. Make sure the rest of transactions are produced after the agent starts:
           ?force_ordering(#{?snk_kind := subscribe_realtime_stream}, #{?snk_kind := trans_gen_counter_update, value := 10}),
           %% 3. Make sure transactions are sent during TLOG replay: (TODO)
           ?force_ordering(#{?snk_kind := state_change, to := bootstrap}, #{?snk_kind := trans_gen_counter_update, value := 15}),
           %% 4. Make sure some transactions are produced while in normal mode
           ?force_ordering(#{?snk_kind := state_change, to := normal}, #{?snk_kind := trans_gen_counter_update, value := 25}),

           Nodes = [N1, N2, N3] = ekka_ct:start_cluster(ekka_async, Cluster),
           ekka_mnesia_test_util:wait_shards([N1, N2]),
           %% Generate some transactions:
           {atomic, _} = rpc:call(N2, ekka_transaction_gen, init, []),
           ok = rpc:call(N1, ekka_transaction_gen, counter, [CounterKey, 30]),
           ekka_mnesia_test_util:stabilize(1000),
           ekka_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           %% Create a delete transaction, to see if deletes are propagated too:
           K = rpc:call(N2, mnesia, dirty_first, [test_tab]),
           {atomic, _} = rpc:call(N2, ekka_transaction_gen, delete, [K]),
           ekka_mnesia_test_util:stabilize(1000),
           [] = rpc:call(N2, mnesia, dirty_read, [test_tab, K]),
           ekka_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           ekka_ct:stop_slave(N3),
           Nodes
       after
           ekka_ct:teardown_cluster(Cluster)
       end,
       fun([N1, N2, N3], Trace) ->
               %% Ensure that the nodes assumed designated roles:
               ?projection_complete(node, ?of_kind(rlog_server_start, Trace), [N1, N2]),
               ?projection_complete(node, ?of_kind(rlog_replica_start, Trace), [N3]),
               %% TODO: Check that some transactions have been buffered during catchup (to increase coverage):
               %?assertMatch([_|_], ?of_kind(rlog_replica_store_trans, Trace)),
               %% Other tests
               ?assert(ekka_rlog_props:replicant_bootstrap_stages(N3, Trace)),
               ?assert(ekka_rlog_props:all_batches_received(Trace)),
               ?assert(ekka_rlog_props:counter_import_check(CounterKey, N3, Trace) > 0)
       end).

t_transaction_on_replicant(_) ->
    Cluster = ekka_ct:cluster([core, replicant], ekka_mnesia_test_util:common_env()),
    ?check_trace(
       try
           Nodes = [N1, N2] = ekka_ct:start_cluster(ekka, Cluster),
           ekka_mnesia_test_util:stabilize(1000),
           {atomic, _} = rpc:call(N2, ekka_transaction_gen, init, []),
           ekka_mnesia_test_util:stabilize(1000), ekka_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           {atomic, KeyVals} = rpc:call(N2, ekka_transaction_gen, ro_read_all_keys, []),
           {atomic, KeyVals} = rpc:call(N1, ekka_transaction_gen, ro_read_all_keys, []),
           Nodes
       after
           ekka_ct:teardown_cluster(Cluster)
       end,
       fun([N1, N2], Trace) ->
               ?assert(ekka_rlog_props:replicant_bootstrap_stages(N2, Trace)),
               ?assert(ekka_rlog_props:all_batches_received(Trace))
       end).

%% Check that behavior on error and exception is the same for both backends
t_abort(_) ->
    Cluster = ekka_ct:cluster([core, replicant], ekka_mnesia_test_util:common_env()),
    ?check_trace(
       try
           Nodes = ekka_ct:start_cluster(ekka, Cluster),
           ekka_mnesia_test_util:wait_shards(Nodes),
           [begin
                RetMnesia = rpc:call(Node, ekka_transaction_gen, abort, [mnesia, AbortKind]),
                RetEkka = rpc:call(Node, ekka_transaction_gen, abort, [ekka_mnesia, AbortKind]),
                case {RetMnesia, RetEkka} of
                    {{aborted, {A, _Stack1}}, {aborted, {A, _Stack2}}} -> ok;
                    {A, A} -> ok
                end
            end
            || Node <- Nodes, AbortKind <- [abort, error, exit, throw]],
           ekka_mnesia_test_util:stabilize(1000),
           ekka_mnesia_test_util:compare_table_contents(test_tab, Nodes)
       after
           ekka_ct:teardown_cluster(Cluster)
       end,
       fun(_, Trace) ->
               ?assertMatch([], ?of_kind(rlog_import_trans, Trace))
       end).

%% Start processes competing for the key on two core nodes and test
%% that updates are received in order
t_core_node_competing_writes(_) ->
    Cluster = ekka_ct:cluster([core, core, replicant], ekka_mnesia_test_util:common_env()),
    CounterKey = counter,
    NOper = 1000,
    ?check_trace(
       try
           Nodes = [N1, N2, N3] = ekka_ct:start_cluster(ekka, Cluster),
           ekka_mnesia_test_util:wait_shards(Nodes),
           spawn(fun() ->
                         rpc:call(N1, ekka_transaction_gen, counter, [CounterKey, NOper]),
                         ?tp(n1_counter_done, #{})
                 end),
           ok = rpc:call(N2, ekka_transaction_gen, counter, [CounterKey, NOper]),
           ?block_until(#{?snk_kind := n1_counter_done}),
           ekka_mnesia_test_util:wait_full_replication(Cluster),
           N3
       after
           ekka_ct:teardown_cluster(Cluster)
       end,
       fun(N3, Trace) ->
               Events = [Val || #{?snk_kind := rlog_import_trans, ops := Ops} <- Trace,
                                {{test_tab, _}, {test_tab, _Key, Val}, write} <- Ops],
               %% Check that the number of imported transaction equals to the expected number:
               ?assertEqual(NOper * 2, length(Events)),
               %% Check that the ops have been imported in order:
               snabbkaffe:strictly_increasing(Events)
       end).

t_rlog_clear_table(_) ->
    snabbkaffe:fix_ct_logging(),
    Cluster = ekka_ct:cluster([core, replicant], ekka_mnesia_test_util:common_env()),
    ?check_trace(
       try
           Nodes = [N1, N2] = ekka_ct:start_cluster(ekka, Cluster),
           ekka_mnesia_test_util:wait_shards(Nodes),
           rpc:call(N1, ekka_transaction_gen, init, []),
           ekka_mnesia_test_util:stabilize(1000),
           ekka_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           ?assertMatch({atomic, ok}, rpc:call(N1, ekka_mnesia, clear_table, [test_tab])),
           ekka_mnesia_test_util:stabilize(1000),
           ekka_mnesia_test_util:compare_table_contents(test_tab, Nodes)
       after
           ekka_ct:teardown_cluster(Cluster)
       end,
       fun(_, _) ->
               true
       end).

t_rlog_dirty_operations(_) ->
    snabbkaffe:fix_ct_logging(),
    Cluster = ekka_ct:cluster([core, core, replicant], ekka_mnesia_test_util:common_env()),
    ?check_trace(
       try
           Nodes = [N1, N2, N3] = ekka_ct:start_cluster(ekka, Cluster),
           ekka_mnesia_test_util:wait_shards(Nodes),
           ok = rpc:call(N1, ekka_mnesia, dirty_write, [{test_tab, 1, 1}]),
           ok = rpc:call(N2, ekka_mnesia, dirty_write, [{test_tab, 2, 2}]),
           ok = rpc:call(N2, ekka_mnesia, dirty_write, [{test_tab, 3, 3}]),
           ekka_mnesia_test_util:stabilize(1000),
           ekka_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           ok = rpc:call(N1, ekka_mnesia, dirty_delete, [test_tab, 1]),
           ok = rpc:call(N2, ekka_mnesia, dirty_delete, [test_tab, 2]),
           ok = rpc:call(N2, ekka_mnesia, dirty_delete, [{test_tab, 3}]),
           ekka_mnesia_test_util:stabilize(1000),
           ekka_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           ?assertMatch(#{ backend        := rlog
                         , role           := replicant
                         , shards_in_sync := [test_shard]
                         , shards_down    := []
                         , shard_stats    := #{test_shard :=
                                                   #{ state               := normal
                                                    , last_imported_trans := _
                                                    , replayq_len         := _
                                                    , upstream            := _
                                                    , bootstrap_time      := _
                                                    , bootstrap_num_keys  := _
                                                    }}
                         }, rpc:call(N3, ekka_rlog, status, []))
       after
           ekka_ct:teardown_cluster(Cluster)
       end,
       fun(_, Trace) ->
               ?assert(ekka_rlog_props:replicant_no_restarts(Trace))
       end).

%% This testcase verifies verifies various modes of ekka_mnesia:ro_transaction
t_sum_verify(_) ->
    Cluster = ekka_ct:cluster([core, replicant], ekka_mnesia_test_util:common_env()),
    NTrans = 100,
    ?check_trace(
       try
           ?force_ordering( #{?snk_kind := verify_trans_step, n := N} when N =:= NTrans div 3
                          , #{?snk_kind := state_change, to := normal}
                          ),
           Nodes = ekka_ct:start_cluster(ekka, Cluster),
           [ok = rpc:call(N, ekka_transaction_gen, verify_trans_sum, [NTrans, 10])
            || N <- lists:reverse(Nodes)],
           [?block_until(#{?snk_kind := verify_trans_sum, node := N}, 5000)
            || N <- Nodes]
       after
           ekka_ct:teardown_cluster(Cluster)
       end,
       fun(_, Trace) ->
               ?assert(ekka_rlog_props:replicant_no_restarts(Trace)),
               ?assertMatch( [#{result := ok}, #{result := ok}]
                           , ?of_kind(verify_trans_sum, Trace)
                           )
       end).

%% Test behavior of the replicant waiting for the core node
t_core_node_down(_) ->
    Cluster = ekka_ct:cluster( [core, core, replicant]
                             , ekka_mnesia_test_util:common_env()
                             ),
    NTrans = 100,
    ?check_trace(
       try
           [N1, N2, N3] = ekka_ct:start_cluster(ekka, Cluster),
           {ok, _} = ?block_until(#{ ?snk_kind := ekka_rlog_status_change
                                   , status := up
                                   , tag := core_node
                                   }),
           %% Stop ekka on all the core nodes:
           {_, {ok, _}} =
               ?wait_async_action(
                  [rpc:call(I, application, stop, [ekka]) || I <- [N1, N2]],
                  #{ ?snk_kind := ekka_rlog_status_change
                   , status    := down
                   , tag       := core_node
                   }),
           %% Restart ekka:
           {_, {ok, _}} =
               ?wait_async_action(
                  [rpc:call(I, application, start, [ekka]) || I <- [N1, N2]],
                  #{ ?snk_kind := ekka_rlog_status_change
                   , status    := up
                   , tag       := core_node
                   }),
           %% Now stop the core nodes:
           {_, {ok, _}} =
               ?wait_async_action(
                  [ekka_ct:stop_slave(I) || I <- [N1, N2]],
                  #{ ?snk_kind := ekka_rlog_status_change
                   , status    := down
                   , tag       := core_node
                   })
       after
           ekka_ct:teardown_cluster(Cluster)
       end,
       fun(_, _Trace) ->
               true
       end).

t_dirty_reads(_) ->
    Cluster = ekka_ct:cluster([core, replicant], ekka_mnesia_test_util:common_env()),
    Key = 1,
    Val = 42,
    ?check_trace(
       #{timetrap => 10000},
       try
           %% Delay shard startup:
           ?force_ordering(#{?snk_kind := read1}, #{?snk_kind := state_change, to := local_replay}),
           [N1, N2] = ekka_ct:start_cluster(ekka_async, Cluster),
           ekka_mnesia_test_util:wait_shards([N1]),
           %% Insert data:
           ok = rpc:call(N1, ekka_mnesia, dirty_write, [{test_tab, Key, Val}]),
           %% Ensure that the replicant still reads the correct value by doing an RPC to the core node:
           ?block_until(#{?snk_kind := rlog_read_from, source := N1}),
           ?assertEqual([{test_tab, Key, Val}], rpc:call(N2, mnesia, dirty_read, [test_tab, Key])),
           %% Now allow the shard to start:
           ?tp(read1, #{}),
           ?block_until(#{?snk_kind := rlog_read_from, source := N2}),
           %% Ensure that the replicant still reads the correct value locally:
           ?assertEqual([{test_tab, Key, Val}], rpc:call(N2, mnesia, dirty_read, [test_tab, Key]))
       after
           ekka_ct:teardown_cluster(Cluster)
       end,
       fun(_, Trace) ->
               ?assert(
                  ?strict_causality( #{?snk_kind := read1}
                                   , #{?snk_kind := state_change, to := normal}
                                   , Trace
                                   ))
       end).

%% Test adding tables to the schema:
t_rlog_schema(_) ->
    snabbkaffe:fix_ct_logging(),
    Cluster = ekka_ct:cluster([core, replicant], ekka_mnesia_test_util:common_env()),
    ?check_trace(
       try
           Nodes = [N1, N2] = ekka_ct:start_cluster(ekka, Cluster),
           ekka_mnesia_test_util:wait_shards(Nodes),
           %% Add a new table
           ?assertMatch( {[ok, ok], []}
                       , rpc:multicall([N1, N2], ekka_mnesia, create_table,
                                       [tab1, [{rlog_shard, test_shard}]])
                       ),
           ok = rpc:call(N1, ekka_mnesia, dirty_write, [{tab1, 1, 1}]),
           %% Check idempotency:
           ?assertMatch( {[ok, ok], []}
                       , rpc:multicall([N1, N2], ekka_mnesia, create_table,
                                       [tab1, [{rlog_shard, test_shard}]])
                       ),
           %% Try to change the shard of an existing table (this should crash):
           ?assertMatch( {[{badrpc, {'EXIT', _}}, {badrpc, {'EXIT', _}}], []}
                       , rpc:multicall([N1, N2], ekka_mnesia, create_table,
                                       [tab1, [{rlog_shard, another_shard}]])
                       ),
           ekka_mnesia_test_util:stabilize(1000),
           ekka_mnesia_test_util:compare_table_contents(tab1, Nodes),
           Nodes
       after
           ekka_ct:teardown_cluster(Cluster)
       end,
       fun([N1, N2], Trace) ->
               ?assert(
                  ?strict_causality( #{?snk_kind := "Adding table to a shard", shard := _Shard, live_change := true}
                                   , #{?snk_kind := "Shard schema change", shard := _Shard}
                                   , ?of_node(N1, Trace)
                                   )),
               ?assert(
                  ?strict_causality( #{?snk_kind := "Shard schema change", shard := _Shard}
                                   , #{?snk_kind := "Restarting RLOG shard", shard := _Shard}
                                   , ?of_node(N1, Trace)
                                   )),
               %% Schema change must cause restart of the replica process and bootstrap:
               {_, Rest} = ?split_trace_at(#{?snk_kind := "Shard schema change"}, Trace),
               ?assert(
                  ?strict_causality( #{?snk_kind := "Restarting RLOG shard", shard := _Shard}
                                   , #{?snk_kind := state_change, to := bootstrap, ?snk_meta := #{node := N2}}
                                   , Rest
                                   ))
       end).

cluster_benchmark(_) ->
    snabbkaffe:fix_ct_logging(),
    NReplicas = 6,
    Config = #{ trans_size => 10
              , max_time   => 15000
              , delays     => [10, 100, 1000]
              },
    ?check_trace(
       begin
           do_cluster_benchmark(Config#{ backend => mnesia
                                       , cluster => [core || I <- lists:seq(1, NReplicas)]
                                       }),
           do_cluster_benchmark(Config#{ backend  => ekka_mnesia
                                       , cluster => [core, core] ++ [replicant || I <- lists:seq(3, NReplicas)]
                                       })
       end,
       fun(_, _) ->
               snabbkaffe:analyze_statistics()
       end).

do_cluster_benchmark(#{ backend    := Backend
                      , trans_size := NKeys
                      , max_time   := MaxTime
                      , delays     := Delays
                      , cluster    := ClusterSpec
                      } = Config) ->
    Env = [ {ekka, rlog_rpc_module, rpc}
          | ekka_mnesia_test_util:common_env()
          ],
    Cluster = ekka_ct:cluster(ClusterSpec, Env),
    ResultFile = "/tmp/" ++ atom_to_list(Backend) ++ "_stats.csv",
    file:write_file( ResultFile
                   , ekka_ct:vals_to_csv([n_nodes | Delays])
                   ),
    [#{node := First}|_] = Cluster,
    try
        Nodes = ekka_ct:start_cluster(node, Cluster),
        ekka_mnesia_test_util:wait_shards(Nodes),
        lists:foldl(
          fun(Node, Cnt) ->
                  ekka_ct:start_ekka(Node),
                  ekka_mnesia_test_util:wait_shards([Node]),
                  ekka_mnesia_test_util:stabilize(100),
                  ok = rpc:call(First, ekka_transaction_gen, benchmark,
                                [ResultFile, Config, Cnt]),
                  Cnt + 1
          end,
          1,
          Cluster)
    after
        ekka_ct:teardown_cluster(Cluster)
    end.
