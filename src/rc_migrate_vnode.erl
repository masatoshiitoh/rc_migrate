-module(rc_migrate_vnode).
-behaviour(riak_core_vnode).
-include("rc_migrate.hrl").
-include("riak_core_vnode.hrl").

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-ignore_xref([
             start_vnode/1
             ]).

%% handover data when join/leave nodes.
-record(state, {partition, data}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, #state { partition=Partition, data=0 }}.

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};
handle_command({add, N}, _Sender, State) ->
    NewData = State#state.data + N,
    {reply, {ok, NewData}, State#state{data=NewData}};
handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.


object_list() -> [].
convtokey(O) -> O.
gotvalue(O) -> O.

%% see http://jp.basho.com/posts/technical/understanding-riak_core-building-handoff/
%% object_list function returns a list of the keys to fold over.
%% write object_list by yourself.
%% TODO Write this.
handle_handoff_command(?FOLD_REQ{foldfun=VisitFun, acc0=Acc0}, _Sender, State) ->
    %% eliding details for now. Don't worry, we'll get to them shortly.
	Do = fun(Object, AccIn) ->
		AccOut = VisitFun({<<"hoge">>, convtokey(Object)}, gotvalue(Object), AccIn)
	end,
    Final = lists:foldl(Do, Acc0, object_list()),
    {reply, Final, State};

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

%% TODO Write this.
handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

%% TODO Write this.
%% see http://jp.basho.com/posts/technical/understanding-riak_core-building-handoff/
%% encode data? encode item?????
encode_handoff_data(_ObjectName, _ObjectValue) ->
    <<>>.

%% TODO Write this.
is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
