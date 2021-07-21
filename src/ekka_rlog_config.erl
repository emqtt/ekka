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

%% @doc Functions for accessing the RLOG configuration
-module(ekka_rlog_config).

-export([ role/0
        , backend/0
        , rpc_module/0
        , strict_mode/0

        , load_config/0

          %% Shard config:
        , load_shard_config/2
        , erase_shard_config/1
        , shard_rlookup/1
        , shard_config/1
        ]).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type raw_config() :: [{ekka_rlog:shard(), [ekka_mnesia:table()]}].

%%================================================================================
%% Persistent term keys
%%================================================================================

-define(shard_rlookup(TABLE), {ekka_shard_rlookup, TABLE}).
-define(shard_config(SHARD), {ekka_shard_config, SHARD}).

-define(ekka(Key), {ekka, Key}).

%%================================================================================
%% API
%%================================================================================

%% @doc Find which shard the table belongs to
-spec shard_rlookup(ekka_mnesia:table()) -> ekka_rlog:shard() | undefined.
shard_rlookup(Table) ->
    persistent_term:get(?shard_rlookup(Table), undefined).

-spec shard_config(ekka_rlog:shard()) -> ekka_rlog:shard_config().
shard_config(Shard) ->
    persistent_term:get(?shard_config(Shard)).

-spec backend() -> ekka_mnesia:backend().
backend() ->
    persistent_term:get(?ekka(db_backend), mnesia).

-spec role() -> ekka_rlog:role().
role() ->
    persistent_term:get(?ekka(node_role), core).

-spec rpc_module() -> gen_rpc | rpc.
rpc_module() ->
    persistent_term:get(?ekka(rlog_rpc_module), gen_rpc).

%% Flag that enables additional verification of transactions
-spec strict_mode() -> boolean().
strict_mode() ->
    persistent_term:get(?ekka(strict_mode), false).

-spec load_config() -> ok.
load_config() ->
    erase_all_config(),
    copy_from_env(rlog_rpc_module),
    copy_from_env(db_backend),
    copy_from_env(node_role),
    copy_from_env(strict_mode).

-spec load_shard_config(ekka_rlog:shard(), [ekka_mnesia:table()]) -> ok.
load_shard_config(Shard, Tables) ->
    %% erase_shard_config(Shard),
    ?tp(notice, "Setting RLOG shard config",
        #{ shard => Shard
         , tables => Tables
         }),
    create_shard_rlookup(Shard, Tables),
    Config = #{ tables => Tables
              , match_spec => make_shard_match_spec(Tables)
              },
    ok = persistent_term:put(?shard_config(Shard), Config).

%%================================================================================
%% Internal
%%================================================================================

-spec copy_from_env(atom()) -> ok.
copy_from_env(Key) ->
    case application:get_env(ekka, Key) of
        {ok, Val} -> persistent_term:put(?ekka(Key), Val);
        undefined -> ok
    end.

%% Create a reverse lookup table for finding shard of the table
-spec create_shard_rlookup(ekka_rlog:shard(), [ekka_mnesia:table()]) -> ok.
create_shard_rlookup(Shard, Tables) ->
    [persistent_term:put(?shard_rlookup(Tab), Shard) || Tab <- Tables],
    ok.

%% Delete persistent terms related to the shard
-spec erase_shard_config(ekka_rlog:shard()) -> ok.
erase_shard_config(Shard) ->
    lists:foreach( fun({Key = ?shard_rlookup(_), S}) when S =:= Shard ->
                           persistent_term:erase(Key);
                      ({Key = ?shard_config(S), _}) when S =:= Shard ->
                           persistent_term:erase(Key);
                      (_) ->
                           ok
                   end
                 , persistent_term:get()
                 ).

%% Delete all the persistent terms created by us
-spec erase_all_config() -> ok.
erase_all_config() ->
    lists:foreach( fun({Key, _}) ->
                           case Key of
                               ?shard_rlookup(_) ->
                                   persistent_term:erase(Key);
                               ?shard_config(_) ->
                                   persistent_term:erase(Key);
                               _ ->
                                   ok
                           end
                   end
                 , persistent_term:get()
                 ).

-spec make_shard_match_spec([ekka_mnesia:table()]) -> ets:match_spec().
make_shard_match_spec(Tables) ->
    [{ {{Table, '_'}, '_', '_'}
     , []
     , ['$_']
     } || Table <- Tables].

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

shard_rlookup_test() ->
    PersTerms = lists:sort(persistent_term:get()),
    try
        ok = load_shard_config(foo, [foo_tab1, foo_tab2]),
        ok = load_shard_config(bar, [bar_tab1, bar_tab2]),
        ?assertMatch(foo, shard_rlookup(foo_tab1)),
        ?assertMatch(foo, shard_rlookup(foo_tab2)),
        ?assertMatch(bar, shard_rlookup(bar_tab1)),
        ?assertMatch(bar, shard_rlookup(bar_tab2))
    after
        erase_all_config(),
        %% Check that erase_all_config function restores the status quo:
        ?assertEqual(PersTerms, lists:sort(persistent_term:get()))
    end.

erase_shard_config_test() ->
    PersTerms = lists:sort(persistent_term:get()),
    try
        ok = load_shard_config(foo, [foo_tab1, foo_tab2])
    after
        erase_shard_config(foo),
        %% Check that erase_all_config function restores the status quo:
        ?assertEqual(PersTerms, lists:sort(persistent_term:get()))
    end.

-endif. %% TEST
