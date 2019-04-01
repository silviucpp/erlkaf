-module(erlkaf_logger).

-include("erlkaf_private.hrl").

-define(RD_LOG_LEVEL_EMERGENCY, 0).
-define(RD_LOG_LEVEL_ALERT, 1).
-define(RD_LOG_LEVEL_CRITICAL, 2).
-define(RD_LOG_LEVEL_ERROR, 3).
-define(RD_LOG_LEVEL_WARNING, 4).
-define(RD_LOG_LEVEL_NOTICE, 5).
-define(RD_LOG_LEVEL_INFO, 6).
-define(RD_LOG_LEVEL_DEBUG, 7).

-export([start_link/0, init/1]).

start_link() ->
    proc_lib:start_link(?MODULE, init, [self()]).

%internals

init(Parent) ->
    Self = self(),
    ok = erlkaf_nif:set_log_process(Self),
    ok = proc_lib:init_ack(Parent, {ok, Self}),
    loop().

loop() ->
    receive
        {log_message, Severity, Name, Fac, Buf} ->
            log_message(Severity, Name, Fac, Buf),
            loop()
    end.

log_message(Severity, Name, Fac, Buf) ->
    Message = format_msg(Name, Fac, Buf),
    case Severity of
        ?RD_LOG_LEVEL_DEBUG ->
            ?LOG_DEBUG(Message, []);
        Info when Info == ?RD_LOG_LEVEL_INFO orelse Info == ?RD_LOG_LEVEL_NOTICE ->
            ?LOG_INFO(Message, []);
        ?RD_LOG_LEVEL_WARNING ->
            ?LOG_WARNING(Message, []);
        ?RD_LOG_LEVEL_ERROR ->
            ?LOG_ERROR(Message, []);
        _ ->
            ?LOG_CRITICAL(Message, [])
    end.

format_msg(Name, Fac, Buf) ->
    <<Name/binary, " ", Fac/binary, " ", Buf/binary>>.