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

%% Internal functions
-module(ekka_rlog_lib).

-export([ approx_checkpoint/0
        , txid_to_checkpoint/1
        , make_key/1
        , import_batch/2
        , rpc_call/4
        , rpc_cast/4
        , shuffle/1
        , send_after/3
        , cancel_timer/1
        ]).

-export_type([ tlog_entry/0
             , subscriber/0
             , table/0
             , change_type/0
             , op/0
             , tx/0
             , mnesia_tid/0
             , txid/0
             , rlog/0
             ]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("ekka_rlog.hrl").
-include_lib("mnesia/src/mnesia.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type mnesia_tid() :: #tid{}.
-type txid() :: {ekka_rlog_server:checkpoint(), pid()}.

-type table():: atom().

-type change_type() :: write | delete | delete_object | clear_table.

-type op() :: {{table(), term()}, term(), change_type()}.

-type dirty() :: {dirty, _Fun :: atom(), _Args :: list()}.

-type tx() :: [op()] | dirty().

-type rlog() :: #rlog{}.

-type tlog_entry() :: { _Sender :: pid()
                      , _SeqNo  :: integer()
                      , _Key    :: txid()
                      , _Tx     :: [tx()]
                      }.

-type subscriber() :: {node(), pid()}.

%%================================================================================
%% RLOG key creation
%%================================================================================

-spec approx_checkpoint() -> ekka_rlog_server:checkpoint().
approx_checkpoint() ->
    erlang:system_time(millisecond).

-spec txid_to_checkpoint(ekka_rlog_lib:txid()) -> ekka_rlog_server:checkpoint().
txid_to_checkpoint({Checkpoint, _}) ->
    Checkpoint.

%% Log key should be globally unique.
%%
%% it is a tuple of a timestamp (ts) and the node id (node_id), where
%% ts is at millisecond precision to ensure it is locally monotonic and
%% unique, and transaction pid, should ensure global uniqueness.
-spec make_key(ekka_rlog_lib:mnesia_tid() | undefined) -> ekka_rlog_lib:txid().
make_key(#tid{pid = Pid}) ->
    {approx_checkpoint(), Pid};
make_key(undefined) ->
    %% This is a dirty operation
    {approx_checkpoint(), make_ref()}.

%% -spec make_key_in_past(integer()) -> ekka_rlog_lib:txid().
%% make_key_in_past(Dt) ->
%%     {TS, Node} = make_key(),
%%     {TS - Dt, Node}.

%%================================================================================
%% Transaction import
%%================================================================================

%% @doc Import transaction ops to the local database
-spec import_batch(transaction | dirty, [tx()]) -> ok.
import_batch(ImportType, Batch) ->
    lists:foreach(fun(Tx) -> import_transaction(ImportType, Tx) end, Batch).

-spec import_transaction(transaction | dirty, tx()) -> ok.
import_transaction(_, {dirty, Fun, Args}) ->
    ?tp(import_dirty_op,
        #{ op    => Fun
         , table => hd(Args)
         , args  => tl(Args)
         }),
    ok = apply(mnesia, Fun, Args);
import_transaction(transaction, Ops) ->
    ?tp(rlog_import_trans,
        #{ type => transaction
         , ops  => Ops
         }),
    {atomic, ok} = mnesia:transaction(
                     fun() ->
                             lists:foreach(fun import_op/1, Ops)
                     end);
import_transaction(dirty, Ops) ->
    ?tp(rlog_import_trans,
        #{ type => dirty
         , ops  => Ops
         }),
    lists:foreach(fun import_op_dirty/1, Ops).

-spec import_op(op()) -> ok.
import_op(Op) ->
    case Op of
        {{Tab, _K}, Record, write} ->
            mnesia:write(Tab, Record, write);
        {{Tab, K}, _Record, delete} ->
            mnesia:delete({Tab, K});
        {{Tab, _K}, Record, delete_object} ->
            mnesia:delete_object(Tab, Record, write);
        {{Tab, _K}, '_', clear_table} ->
            ekka_rlog_activity:clear_table(Tab)
    end.

-spec import_op_dirty(op()) -> ok.
import_op_dirty({{Tab, '_'}, '_', clear_table}) ->
    mnesia:clear_table(Tab);
import_op_dirty({{Tab, _K}, Record, delete_object}) ->
    mnesia:dirty_delete_object(Tab, Record);
import_op_dirty({{Tab, K}, _Record, delete}) ->
    mnesia:dirty_delete({Tab, K});
import_op_dirty({{Tab, _K}, Record, write}) ->
    mnesia:dirty_write(Tab, Record).

%%================================================================================
%% RPC
%%================================================================================

%% @doc Do an RPC call
-spec rpc_call(node(), module(), atom(), list()) -> term().
rpc_call(Node, Module, Function, Args) ->
    Mod = ekka_rlog_config:rpc_module(),
    apply(Mod, call, [Node, Module, Function, Args]).

%% @doc Do an RPC cast
-spec rpc_cast(node(), module(), atom(), list()) -> term().
rpc_cast(Node, Module, Function, Args) ->
    Mod = ekka_rlog_config:rpc_module(),
    apply(Mod, cast, [Node, Module, Function, Args]).

%%================================================================================
%% Misc functions
%%================================================================================

%% @doc Random shuffle of a small list.
-spec shuffle([A]) -> [A].
shuffle(L0) ->
    {_, L} = lists:unzip(lists:sort([{rand:uniform(), I} || I <- L0])),
    L.

-spec send_after(timeout(), pid(), _Message) -> reference() | undefined.
send_after(infinity, _, _) ->
    undefined;
send_after(Timeout, To, Message) ->
    erlang:send_after(Timeout, To, Message).

-spec cancel_timer(reference() | undefined) -> ok.
cancel_timer(undefined) ->
    ok;
cancel_timer(TRef) ->
    erlang:cancel_timer(TRef).

%%================================================================================
%% Internal
%%================================================================================
