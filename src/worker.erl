-module(worker).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).
-export([stop/1]).

-export([add/2]).
-export([set_state/2]).
-export([get_state/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?MODULE, [], []).

stop(Pid) ->
    gen_server:call(Pid, stop).

add(Pid, N) -> gen_server:call(Pid, {add, N}).

set_state(Pid, N) -> gen_server:call(Pid, {set_state, N}).

get_state(Pid) -> gen_server:call(Pid, get_state).
%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_Args) ->
    {ok, 0}.

handle_call({add, N}, _From, N1) ->
    {reply, {ok, N+N1}, N+N1};

handle_call({set_state, N}, _From, _N1) ->
    {reply, {ok, N} , N};

handle_call(get_state, _From, N1) ->
    {reply, {ok, N1}, N1};

handle_call(stop, _From, N1) ->
    {stop, normal, stopped, N1};

handle_call(_Request, _From, N1) ->
    {reply, ok, N1}.

handle_cast(_Msg, N1) ->
    {noreply, N1}.

handle_info(_Info, N1) ->
    {noreply, N1}.

terminate(_Reason, _N1) ->
    ok.

code_change(_OldVsn, N1, _Extra) ->
    {ok, N1}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

