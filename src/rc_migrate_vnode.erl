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
handle_command({new, Name}, _Sender, State) ->
	{ok, Pid} = worker:start_link(),
	NewPids = dict:store(Name, Pid, State#state.pids),
	NewState = State#state{pids = NewPids},
	?PRINT({new, Name}),
	{reply, {{new, Pid}, State#state.partition}, NewState};

handle_command({add, Name, N}, _Sender, State) ->
	Pid = dict:fetch(Name, State#state.pids),
	{ok, Result} = worker:add(Pid, N),
	{reply, {ok, Result}, State};

handle_command({get_state, Name}, _Sender, State) ->
	Pid = dict:fetch(Name, State#state.pids),
	{ok, Result} = worker:get_state(Pid),
	{reply, {ok, Result}, State};

handle_command({set_state, Name, S}, _Sender, State) ->
	Pid = dict:fetch(Name, State#state.pids),
	{ok, Result} = worker:set_state(Pid, S),
	{reply, {ok, Result}, State};

handle_command(Message, _Sender, State) ->
	?PRINT({unhandled_command, Message}),
	{noreply, State}.


object_list(State) ->
	dict:fetch_keys(State#state.pids).

convtokey(Name) -> {<<"rc_migrate">>, list_to_binary(Name)}.

gotvalue(Name, State) ->
	{reply, {ok, Result}, State} = handle_command({get_state, Name}, self(), State),
	Result.

%% see http://jp.basho.com/posts/technical/understanding-riak_core-building-handoff/
%% object_list function returns a list of the keys to fold over.
%% write object_list by yourself.
handle_handoff_command(?FOLD_REQ{foldfun=VisitFun, acc0=Acc0}, _Sender, State) ->
	%% eliding details for now. Don't worry, we'll get to them shortly.
	Do = fun(Object, AccIn) ->
		K = convtokey(Object),
		?PRINT(K),
		V = gotvalue(Object, State),
		?PRINT(V),
		AccOut = VisitFun(K, V, AccIn),
		?PRINT(AccOut),
		AccOut
	end,
	Final = lists:foldl(Do, Acc0, object_list(State)),
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
	{Key, Value} = binary_to_term(Data),
	{reply, {ok, Result}, State1} = handle_command({add, Key}, self(), State),
	{reply, {ok, Result}, State2} = handle_command({set_state, Key, Value}, self(), State1),
	{reply, ok, State2}.

encode_handoff_item(Key, Value) ->
	term_to_binary({Key, Value}).

is_empty(State) ->
	{dict:size(State#state.pids) == 0, State}.

delete(State) ->
	{ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
	{stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.
