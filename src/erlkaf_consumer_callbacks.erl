-module(erlkaf_consumer_callbacks).

-include("erlkaf.hrl").

-callback init(binary(), integer(), integer(), any()) ->
    {ok, any()}.

-callback handle_message(#erlkaf_msg{}, state()) ->
    {ok, state()} | {error, reason(), state()}.

-callback stats_callback(client_id(), list()) ->
    ok.

-optional_callbacks([stats_callback/2]).
