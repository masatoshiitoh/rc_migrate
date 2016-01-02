-module(rc_migrate).
-include("rc_migrate.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
         add/1
        ]).

-ignore_xref([
              ping/0,
              add/1
             ]).

%% Public API

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, rc_migrate),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, rc_migrate_vnode_master).

add(N) ->
    DocIdx = riak_core_util:chash_key({<<"service">>, <<"add">>}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, rc_migrate),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {add, N}, rc_migrate_vnode_master).
