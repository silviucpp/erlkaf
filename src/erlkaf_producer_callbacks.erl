-module(erlkaf_producer_callbacks).

-include("erlkaf.hrl").

-callback delivery_report(DeliveryStatus:: ok | {error, any()}, Message::#erlkaf_msg{}) ->
    ok.

-callback stats_callback(ClientId::client_id(), Stats::map()) ->
    ok.

-optional_callbacks([stats_callback/2, delivery_report/2]).
