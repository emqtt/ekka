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
         notify_shard_down/1, wait_for_shards/2, upstream/1]).

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
-define(upstream_node, upstream_node).

%%================================================================================
%% API funcions
%%================================================================================

%% @doc Return core node used as the upstream for the replica
-spec upstream(ekka_rlog:shard()) -> {ok, node()} | disconnected.
upstream(Shard) ->
    case ets:lookup(?replica_tab, {?upstream_node, Shard}) of
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
    gen_event:start_link({local, ?SERVER}, []).

-spec notify_shard_up(ekka_rlog:shard(), node()) -> ok.
notify_shard_up(Shard, Upstream) ->
    ?tp(notify_shard_up,
        #{ shard => Shard
         , node  => node()
         }),
    ets:insert(?replica_tab, {{?upstream_node, Shard}, Upstream}),
    gen_event:notify(?SERVER, {shard_up, Shard}).

-spec notify_shard_down(ekka_rlog:shard()) -> ok.
notify_shard_down(Shard) ->
    ets:delete(?replica_tab, {?upstream_node, Shard}),
    ?tp(notify_shard_down,
        #{ shard => Shard
         }).

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
    ERef = subscribe_events(),
    TRef = ekka_rlog_lib:send_after(Timeout, self(), {ERef, timeout}),
    %% Exclude shards that are up, since they are not going to send any events:
    DownShards = Shards -- lists:append(ets:match(?replica_tab, {{?upstream_node, '$1'}, '_'})),
    Ret = do_wait_shards(ERef, DownShards),
    ekka_rlog_lib:cancel_timer(TRef),
    unsubscribe_events(ERef),
    ?tp(notice, "Done waiting for shards",
        #{ shards => Shards
         , result =>  Ret
         }),
    Ret.

%%================================================================================
%% gen_event callbacks
%%================================================================================

init([Ref, Subscriber]) ->
    logger:set_process_metadata(#{domain => [ekka, rlog, event_mgr]}),
    ?tp(start_event_monitor,
        #{ reference => Ref
         , subscriber => Subscriber
         }),
    State = #s{ ref = Ref
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

do_wait_shards(_, []) ->
    ok;
do_wait_shards(ERef, RemainingShards) ->
    receive
        {'DOWN', ERef, _, _, _} ->
            error(rlog_restarted);
        {ERef, Event} ->
            case Event of
                {shard_up, Shard} ->
                    do_wait_shards(ERef, RemainingShards -- [Shard]);
                timeout ->
                    {timeout, RemainingShards};
                _ ->
                    do_wait_shards(ERef, RemainingShards)
            end
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
