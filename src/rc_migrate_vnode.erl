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
-record(state, {partition, data, pids}).

%% API
start_vnode(I) ->
	riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
	{ok, #state { partition=Partition, data=0, pids=dict:new() }}.

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
	{reply, {pong, State#state.partition}, State};

%% Name is new comer.
handle_command({new, BinName}, _Sender, State) ->
	{ok, Pid} = worker:start_link(),
	NewPids = dict:store(BinName, Pid, State#state.pids),
	NewState = State#state{pids = NewPids},
	?PRINT({new, BinName}),
	{reply, {ok, {new, Pid}, State#state.partition}, NewState};

handle_command({add, BinName, N}, _Sender, State) ->
	Pid = dict:fetch(BinName, State#state.pids),
	{ok, Result} = worker:add(Pid, N),
	{reply, {ok, Result}, State};

handle_command({get_state, BinName}, _Sender, State) ->
	Pid = dict:fetch(BinName, State#state.pids),
	{ok, Result} = worker:get_state(Pid),
	{reply, {ok, Result}, State};

handle_command({set_state, BinName, S}, _Sender, State) ->
	Pid = dict:fetch(BinName, State#state.pids),
	{ok, NewWorkerState} = worker:set_state(Pid, S),
	{reply, {ok, NewWorkerState}, State};

handle_command(Message, _Sender, State) ->
	?PRINT({unhandled_command, Message}),
	{noreply, State}.


%% get list of binnames
object_list(State) ->
	dict:fetch_keys(State#state.pids).

make_key(BinName) ->
	{<<"rc_migrate">>, BinName}.

get_value(BinName, State) ->
	local_get_state(BinName, State).

%% see http://jp.basho.com/posts/technical/understanding-riak_core-building-handoff/
%% object_list function returns a list of the keys to fold over.
%% write object_list by yourself.
handle_handoff_command(?FOLD_REQ{foldfun=VisitFun, acc0=Acc0}, _Sender, State) ->
	ObjectBinNames = object_list(State),
	%% eliding details for now. Don't worry, we'll get to them shortly.
	Do = fun(BinName, AccIn) ->
		K = make_key(BinName),
		?PRINT(K),
		V = get_value(BinName, State),
		?PRINT(V),
		%%Data = term_to_binary({K, V}),
		Data = V, %% don't apply XXX_to_binary here.
		?PRINT(Data),
		AccOut = VisitFun(K, Data, AccIn),
		?PRINT(AccOut),
		AccOut
	end,
	Final = lists:foldl(Do, Acc0, ObjectBinNames),
	{reply, Final, State};

handle_handoff_command(_Message, _Sender, State) ->
	{noreply, State}.

handoff_starting(_TargetNode, State) ->
	{true, State}.

handoff_cancelled(State) ->
	{ok, State}.

handoff_finished(_TargetNode, State) ->
	{ok, State}.

handle_handoff_data(Data, State) ->
	{{<<"rc_migrate">>, BinName} , WorkerState} = binary_to_term(Data),
	?PRINT({BinName, WorkerState}),
	NewState = case dict:find(BinName, State#state.pids) of
		{ok, ExistingPid} ->
			{ok, _NewWorkerState} = worker:set_state(ExistingPid, WorkerState),
			State;
		error ->
			?PRINT({BinName, WorkerState}),
			start_and_set_state({BinName, WorkerState}, State)
	end,
	{reply, ok, NewState}.

%% returns State
start_and_set_state({BinName, WorkerState}, State) ->
	?PRINT({start_and_set_state, BinName, WorkerState}),
	{ok, Pid} = worker:start_link(),
	{ok, _NewWorkerState} = worker:set_state(Pid, WorkerState),
	NewPids = dict:store(BinName, Pid, State#state.pids),
	NewState = State#state{pids = NewPids},
	NewState.

local_get_state(BinName, State) ->
	Pid = dict:fetch(BinName, State#state.pids),
	{ok, Result} = worker:get_state(Pid),
	Result.

encode_handoff_item(BinKey, Value) ->
	term_to_binary({BinKey, Value}).

is_empty(State) ->
	{dict:size(State#state.pids) == 0, State}.

delete(State) ->
	?PRINT({delete, object_list(State)}),
	NewState = stop_proc(object_list(State), State),
	?PRINT({deleted, NewState}),
	{ok, NewState}.

stop_proc([], State) -> State;

stop_proc([BinName|L], State) ->
	Pid = dict:fetch(BinName, State#state.pids),
	_Result = stopped = worker:stop(Pid),
	?PRINT({worker_stop_result, _Result}),
	NewDict = dict:erase(BinName, State#state.pids),
	stop_proc(L, State#state{pids = NewDict}).

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
	{stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.
