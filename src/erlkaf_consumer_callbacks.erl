-module(erlkaf_consumer_callbacks).

-include("erlkaf.hrl").

-callback init(binary(), integer(), integer(), any()) ->
    {ok, any()}.

-callback handle_message(#erlkaf_msg{}, state()) ->
    {ok, state()} | {error, reason(), state()}.

-callback handle_failed_message(#erlkaf_msg{}, state()) ->
    {ok, state()}.

-callback stats_callback(client_id(), map()) ->
    ok.

-callback oauthbearer_token_refresh_callback(binary()) ->
    ok.

-optional_callbacks([
    stats_callback/2,
    handle_failed_message/2,
    oauthbearer_token_refresh_callback/1
]).
