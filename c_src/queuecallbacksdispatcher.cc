#include "queuecallbacksdispatcher.h"
#include "rdkafka.h"

#include <chrono>
#include <limits>

namespace  {
    void consumers_event_callback(rd_kafka_t *rk, void *qev_opaque)
    {
        static_cast<QueueCallbacksDispatcher*>(qev_opaque)->signal(rk);
    }

    inline uint64_t now_ms()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    }
}

QueueCallbacksDispatcher::QueueCallbacksDispatcher():
    running_(true),
    poll_timeout_(-1)
{
    thread_callbacks_ = std::thread(&QueueCallbacksDispatcher::process_callbacks, this);
}

QueueCallbacksDispatcher::~QueueCallbacksDispatcher()
{
    ASSERT(objects_.empty());
    running_ = false;
    events_.enqueue(nullptr);
    thread_callbacks_.join();
}

void QueueCallbacksDispatcher::watch(rd_kafka_t* instance, bool is_consumer)
{
     char buffer[256];
     size_t buffer_size = sizeof(buffer);

    const rd_kafka_conf_t* conf = rd_kafka_conf(instance);
    rd_kafka_conf_get(conf, "max.poll.interval.ms", buffer, &buffer_size);
    ASSERT(buffer_size > 0);
    uint64_t max_poll_interval_ms = static_cast<uint64_t>(std::stoull(buffer)/2);

    {
        CritScope ss(&crt_);
        objects_[instance] = item(is_consumer, max_poll_interval_ms);

        // poll timeout is the minimum value of all instances

        for(auto& it: objects_)
        {
            if(it.second.max_poll_interval_ms < max_poll_interval_ms)
                max_poll_interval_ms = it.second.max_poll_interval_ms;
        }

        poll_timeout_ = static_cast<int64_t>(max_poll_interval_ms);
    }

    rd_kafka_queue_t* queue = is_consumer ? rd_kafka_queue_get_consumer(instance): rd_kafka_queue_get_main(instance);
    rd_kafka_queue_cb_event_enable(queue, consumers_event_callback, this);
    rd_kafka_queue_destroy(queue);
}

bool QueueCallbacksDispatcher::remove(rd_kafka_t* instance)
{
    bool is_consumer;

    {
        CritScope ss(&crt_);

        auto it = objects_.find(instance);

        if(it == objects_.end())
            return false;

        is_consumer = it->second.is_consumer;
        objects_.erase(it);

        // recalculate the minimum polling time.

        if(objects_.empty() == false)
        {
            uint64_t max_poll_interval_ms = std::numeric_limits<int64_t>::max();

            for(auto& it: objects_)
            {
                if(it.second.max_poll_interval_ms < max_poll_interval_ms)
                    max_poll_interval_ms = it.second.max_poll_interval_ms;
            }

            poll_timeout_ = static_cast<int64_t>(max_poll_interval_ms);
        }
        else
        {
            poll_timeout_ = -1;
        }
    }

    rd_kafka_queue_t* queue = is_consumer ? rd_kafka_queue_get_consumer(instance): rd_kafka_queue_get_main(instance);
    rd_kafka_queue_cb_event_enable(queue, NULL, nullptr);
    rd_kafka_queue_destroy(queue);
    return true;
}

void QueueCallbacksDispatcher::signal(rd_kafka_t* instance)
{
    events_.enqueue(instance);
}

void QueueCallbacksDispatcher::process_callbacks()
{
    rd_kafka_t* obj = nullptr;

    while (running_)
    {
        if(events_.wait_dequeue_timed(obj, poll_timeout_) == false)
        {
            uint64_t now = now_ms();
            CritScope ss(&crt_);
            check_max_poll_interval_ms(now);
            continue;
        }

        if(obj == nullptr)
            continue;

        uint64_t now = now_ms();

        CritScope ss(&crt_);

        auto it = objects_.find(obj);

        if(it != objects_.end())
        {
            it->second.last_poll_ms = now;
            do_poll(obj, it->second.is_consumer);
        }

        check_max_poll_interval_ms(now);
    }
}

void QueueCallbacksDispatcher::check_max_poll_interval_ms(uint64_t now)
{
    for(auto& it: objects_)
    {
        if(now - it.second.last_poll_ms >= it.second.max_poll_interval_ms)
        {
            it.second.last_poll_ms = now;
            do_poll(it.first, it.second.is_consumer);
        }
    }
}

void QueueCallbacksDispatcher::do_poll(rd_kafka_t* obj, bool is_consumer)
{
    if(is_consumer)
    {
        //consumer polling

        rd_kafka_message_t* msg = nullptr;
        while(running_ && ((msg = rd_kafka_consumer_poll(obj, 0)) != nullptr))
        {
            // because communication between nif and erlang it's based on async messages might be a small window
            // between starting of revoking partitions (queued are forwarded back on the main queue) and when actual we revoked them
            // when we get the messages here. we drop all this messages as time they have no impact because offset is not changed.
            // we are sleeping here as well to not consume lot of cpu
            rd_kafka_message_destroy(msg);
        }
    }
    else
    {
        //producer polling
        rd_kafka_poll(obj, 0);
    }
}
