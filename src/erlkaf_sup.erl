-module(erlkaf_sup).

-behaviour(supervisor).

-export([
    start_link/0,
    init/1,
    add_client/3,
    remove_client/1
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

add_client(ClientId, Module, Args) ->
    ChildSpecs = build_childspec(ClientId, Module, infinity, Args),
    supervisor:start_child(?MODULE, ChildSpecs).

remove_client(ClientId) ->
    case supervisor:terminate_child(?MODULE, ClientId) of
        ok ->
            supervisor:delete_child(?MODULE, ClientId);
        Error ->
            Error
    end.

init([]) ->
    Children = [
        proccess(erlkaf_logger, infinity),
        proccess(erlkaf_manager, infinity)
    ],

    {ok, {{one_for_one, 20, 1}, Children}}.

proccess(Name, WaitForClose) ->
    {Name, {Name, start_link, []}, permanent, WaitForClose, worker, [Name]}.

build_childspec(Name, Module, WaitForClose, Args) ->
    {Name, {Module, start_link, Args}, transient, WaitForClose, worker, [Module]}.
