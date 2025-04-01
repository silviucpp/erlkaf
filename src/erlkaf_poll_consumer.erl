-module(erlkaf_poll_consumer).

-include("erlkaf_private.hrl").

-define(DEFAULT_BATCH_SIZE, 100).

-behaviour(gen_server).

-export([
    start_link/6,
    stop/1,

    % gen_server

    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-record(state, {
    client_ref,
    topic_name,
    partition,
    queue_ref,
    poll_batch_size
}).

start_link(ClientRef, TopicName, Partition, Offset, QueueRef, TopicSettings) ->
    gen_server:start_link(
        ?MODULE, [ClientRef, TopicName, Partition, Offset, QueueRef, TopicSettings], []
    ).

stop(Pid) ->
    case erlang:is_process_alive(Pid) of
        true ->
            Tag = make_ref(),
            Pid ! {stop, self(), Tag},

            receive
                {stopped, Tag} ->
                    ok
            after 5000 ->
                exit(Pid, kill)
            end;
        _ ->
            {error, not_alive}
    end.

init([ClientRef, TopicName, Partition, Offset, QueueRef, TopicSettings]) ->
    ?LOG_DEBUG("start poll consumer for: ~p partition: ~p offset: ~p", [
        TopicName, Partition, Offset
    ]),

    PollBatchSize = erlkaf_utils:lookup(poll_batch_size, TopicSettings, ?DEFAULT_BATCH_SIZE),

    {ok, #state{
        client_ref = ClientRef,
        topic_name = TopicName,
        partition = Partition,
        queue_ref = QueueRef,
        poll_batch_size = PollBatchSize
    }}.

handle_call(poll, _From, #state{queue_ref = Queue, poll_batch_size = PollBatchSize} = State) ->
    case erlkaf_nif:consumer_queue_poll(Queue, PollBatchSize) of
        {ok, Events, LastOffset} ->
            {reply, {ok, Events, LastOffset}, State};
        Error ->
            ?LOG_INFO("~p poll events error: ~p", [?MODULE, Error]),
            throw({error, Error})
    end;

handle_call(Request, _From, State) ->
    ?LOG_ERROR("handle_call unexpected message: ~p", [Request]),
    {reply, ok, State}.

handle_cast({commit_offset, Offset}, #state{
        client_ref = ClientRef,
        topic_name = Topic,
        partition = Partition} = State) ->
    erlkaf_nif:consumer_offset_store(ClientRef, Topic, Partition, Offset),
    {noreply, State};

handle_cast(Request, State) ->
    ?LOG_ERROR("handle_cast unexpected message: ~p", [Request]),
    {noreply, State}.

handle_info({stop, From, Tag}, State) ->
    handle_stop(From, Tag, State),
    {stop, normal, State}.

handle_stop(From, Tag, #state{topic_name = TopicName, partition = Partition}) ->
    ?LOG_INFO("stop poll consumer for: ~p partition: ~p", [TopicName, Partition]),
    From ! {stopped, Tag}.
