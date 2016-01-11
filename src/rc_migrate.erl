-module(rc_migrate).
-include("rc_migrate.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
         new/1,
         add/2,
         get_state/1,
         set_state/2
        ]).

-ignore_xref([
              ping/0,
              new/1,
              add/2,
	      get_state/1,
	      set_state/2
             ]).

%% Public API

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, rc_migrate),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, rc_migrate_vnode_master).

new(Name) ->
	BinName = list_to_binary(Name),
    DocIdx = riak_core_util:chash_key({<<"rc_migrate">>, BinName}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, rc_migrate),
    [{IndexNode, _Type}] = PrefList,
	?PRINT(IndexNode),
    riak_core_vnode_master:sync_spawn_command(IndexNode, {new, BinName}, rc_migrate_vnode_master).

add(Name, N) ->
	BinName = list_to_binary(Name),
    DocIdx = riak_core_util:chash_key({<<"rc_migrate">>, BinName}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, rc_migrate),
    [{IndexNode, _Type}] = PrefList,
	?PRINT(IndexNode),
    riak_core_vnode_master:sync_spawn_command(IndexNode, {add, BinName, N}, rc_migrate_vnode_master).

get_state(Name) ->
	BinName = list_to_binary(Name),
    DocIdx = riak_core_util:chash_key({<<"rc_migrate">>, BinName}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, rc_migrate),
    [{IndexNode, _Type}] = PrefList,
	?PRINT(IndexNode),
    riak_core_vnode_master:sync_spawn_command(IndexNode, {get_state, BinName}, rc_migrate_vnode_master).

set_state(Name, S) ->
	BinName = list_to_binary(Name),
    DocIdx = riak_core_util:chash_key({<<"rc_migrate">>, BinName}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, rc_migrate),
    [{IndexNode, _Type}] = PrefList,
	?PRINT(IndexNode),
    riak_core_vnode_master:sync_spawn_command(IndexNode, {set_state, BinName, S}, rc_migrate_vnode_master).
