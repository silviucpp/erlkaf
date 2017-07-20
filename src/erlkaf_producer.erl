-module(erlkaf_producer).

-include("erlkaf_private.hrl").
-include("erlkaf.hrl").

-behaviour(gen_server).

-export([start_link/3]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-callback delivery_report(MsgRef::reference(), DeliveryStatus:: ok| {error, any()}, Message::#erlkaf_msg{}) ->
    ok.

-record(state, {
    ref,
    dr_callback
}).

start_link(ClientId, DrCallback, ProducerRef) ->
    gen_server:start_link(?MODULE, [ClientId, DrCallback, ProducerRef], []).

init([ClientId, DrCallback, ProducerRef]) ->
    Pid = self(),
    ok = erlkaf_nif:producer_set_owner(ProducerRef, Pid),
    ok = erlkaf_cache_client:set(ClientId, ProducerRef, Pid),
    {ok, #state{ref = ProducerRef, dr_callback = DrCallback}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({delivery_report, MsgRef, DeliveryStatus, Message}, #state{dr_callback = Callback} = State) ->
    case Callback of
        undefined ->
            ok;
        _ ->
            case catch call_callback(Callback, MsgRef, DeliveryStatus, Message) of
                ok ->
                    ok;
                Error ->
                    ?ERROR_MSG("~p:delivery_report error: ~p", [Callback, Error])
            end
    end,
    {noreply, State};
handle_info(Info, State) ->
    ?ERROR_MSG("received unknown message: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%internals

call_callback(C, MsgRef, DeliveryStatus, Message) when is_function(C, 3) ->
    C(MsgRef, DeliveryStatus, Message);
call_callback(C, MsgRef, DeliveryStatus, Message) ->
    C:delivery_report(MsgRef, DeliveryStatus, Message).
