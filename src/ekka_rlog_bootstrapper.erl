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

%% @doc This module implements both bootstrap server and client

-module(ekka_rlog_bootstrapper).

-behavior(gen_server).

%% API:
-export([start_link/2, start_link_client/3]).

%% gen_server callbacks:
-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%% Internal exports:
-export([do_push_batch/2, do_complete/3]).

-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type batch() :: { _From    :: pid()
                 , _Table   :: ekka_mnesia:table()
                 , _Records :: [tuple()]
                 }.

-record(server,
        { shard       :: ekka_rlog:shard()
        , subscriber  :: ekka_rlog_lib:subscriber()
        , key_queue   :: replayq:q() | undefined
        , tables      :: [ekka_mnesia:table()]
        }).

-record(client,
        { shard       :: ekka_rlog:shard()
        , server      :: pid()
        , parent      :: pid()
        }).

%%================================================================================
%% API funcions
%%================================================================================

%% @doc Start bootstrapper server
-spec start_link(ekka_rlog:shard(), ekka_rlog_lib:subscriber()) -> {ok, pid()}.
start_link(Shard, Subscriber) ->
    gen_server:start_link(?MODULE, {server, Shard, Subscriber}, []).

%% @doc Start bootstrapper client
-spec start_link_client(ekka_rlog:shard(), node(), pid()) -> {ok, pid()}.
start_link_client(Shard, RemoteNode, Parent) ->
    gen_server:start_link(?MODULE, {client, Shard, RemoteNode, Parent}, []).

%%================================================================================
%% Internal exports (gen_rpc)
%%================================================================================

-spec do_push_batch(pid(), batch()) -> ok.
do_push_batch(Pid, Batch) ->
    gen_server:call(Pid, {batch, Batch}, infinity).

-spec do_complete(pid(), pid(), ekka_rlog_server:checkpoint()) -> ok.
do_complete(Client, Server, Snapshot) ->
    gen_server:call(Client, {complete, Server, Snapshot}, infinity).

%%================================================================================
%% gen_server callbacks
%%================================================================================

init({server, Shard, Subscriber}) ->
    logger:set_process_metadata(#{ domain => [ekka, rlog, bootstrapper, server]
                                 , shard  => Shard
                                 }),
    #{tables := Tables} = ekka_rlog_config:shard_config(Shard),
    ?tp(info, rlog_bootstrapper_start,
        #{ shard     => Shard
         , subscribe => Subscriber
         }),
    Queue = replayq:open(#{ mem_only => true
                          , sizer    => fun(_) -> 1 end
                          }),
    self() ! table_loop,
    {ok, #server{ shard      = Shard
                , subscriber = Subscriber
                , tables     = Tables
                , key_queue  = Queue
                }};
init({client, Shard, RemoteNode, Parent}) ->
    logger:set_process_metadata(#{ domain => [ekka, rlog, bootstrapper, client]
                                 , shard  => Shard
                                 }),
    ekka_rlog_status:notify_replicant_bootstrap_start(Shard),
    {ok, Pid} = ekka_rlog_server:bootstrap_me(RemoteNode, Shard),
    {ok, #client{ parent     = Parent
                , shard      = Shard
                , server     = Pid
                }}.

handle_info(table_loop, St = #server{}) ->
    start_table_traverse(St);
handle_info(chunk_loop, St = #server{tables = [_|_]}) ->
    traverse_queue(St);
handle_info(_Info, St) ->
    {noreply, St}.

handle_cast(_Cast, St) ->
    {noreply, St}.

handle_call({complete, Server, Checkpoint}, From, St = #client{server = Server, parent = Parent, shard = Shard}) ->
    ?tp(info, shard_bootstrap_complete, #{}),
    Parent ! {bootstrap_complete, self(), Checkpoint},
    gen_server:reply(From, ok),
    ekka_rlog_status:notify_replicant_bootstrap_complete(Shard),
    {stop, normal, St};
handle_call({batch, {Server, Table, Records}}, _From, St = #client{server = Server, shard = Shard}) ->
    handle_batch(Table, Records),
    ekka_rlog_status:notify_replicant_bootstrap_import(Shard),
    {reply, ok, St};
handle_call(Call, _From, St) ->
    {reply, {error, {unknown_call, Call}}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

terminate(_Reason, St = #server{key_queue = Q}) ->
    replayq:close(Q),
    {ok, St};
terminate(_Reason, St = #client{}) ->
    {ok, St}.

%%================================================================================
%% Internal functions
%%================================================================================

-spec push_batch(ekka_rlog_lib:subscriber(), batch()) -> ok | {badrpc, _}.
push_batch({Node, Pid}, Batch = {_, _, _}) ->
    ekka_rlog_lib:rpc_call(Node, ?MODULE, do_push_batch, [Pid, Batch]).

-spec complete(ekka_rlog_lib:subscriber(), pid(), ekka_rlog_server:checkpoint()) -> ok.
complete({Node, Pid}, Server, Checkpoint) ->
    ekka_rlog_lib:rpc_call(Node, ?MODULE, do_complete, [Pid, Server, Checkpoint]).

handle_batch(Table, Records) ->
    lists:foreach(fun(I) -> mnesia:dirty_write(Table, I) end, Records).

start_table_traverse(St = #server{tables = [], subscriber = Subscriber}) ->
    _ = complete(Subscriber, self(), ekka_rlog_lib:approx_checkpoint()),
    {stop, normal, St};
start_table_traverse(St0 = #server{ shard = Shard
                                  , tables = [Table|_Rest]
                                  , key_queue = Q0
                                  }) ->
    ?tp(info, start_shard_table_bootstrap,
        #{ shard => Shard
         , table => Table
         }),
    Q = replayq:append(Q0, mnesia:dirty_all_keys(Table)),
    St = St0#server{ key_queue = Q },
    self() ! chunk_loop,
    {noreply, St}.

traverse_queue(St0 = #server{key_queue = Q0, subscriber = Subscriber, tables = [Table|Rest]}) ->
    ChunkConfig = application:get_env(ekka, bootstrapper_chunk_config, #{}),
    {Q, AckRef, Items} = replayq:pop(Q0, ChunkConfig),
    Records = prepare_batch(Table, Items),
    Batch = {self(), Table, Records},
    case push_batch(Subscriber, Batch) of
        ok ->
            ok = replayq:ack(Q, AckRef),
            case replayq:is_empty(Q) of
                true ->
                    self() ! table_loop,
                    St = St0#server{tables = Rest},
                    {noreply, St};
                false ->
                    self() ! chunk_loop,
                    {noreply, St0#server{key_queue = Q}}
            end;
        {badrpc, Err} ->
            ?tp(warning, "Failed to push batch",
                #{ subscriber => Subscriber
                 , reason     => Err
                 }),
            {stop, normal, St0}
    end.

-spec prepare_batch(ekka_mnesia:table(), list()) -> [tuple()].
prepare_batch(Table, Keys) ->
    lists:foldl( fun(Key, Acc) -> mnesia:dirty_read(Table, Key) ++ Acc end
               , []
               , Keys
               ).
