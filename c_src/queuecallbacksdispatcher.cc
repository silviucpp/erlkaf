#include "queuecallbacksdispatcher.h"
#include "rdkafka.h"

namespace  {
    void consumers_event_callback(rd_kafka_t *rk, void *qev_opaque)
    {
        static_cast<QueueCallbacksDispatcher*>(qev_opaque)->signal(rk);
    }
}

QueueCallbacksDispatcher::QueueCallbacksDispatcher()
{
    running_ = true;
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
    {
        CritScope ss(&crt_);
        objects_[instance] = is_consumer;
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

        is_consumer = it->second;
        objects_.erase(it);
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
    rd_kafka_t* obj;

    while (running_)
    {
        events_.wait_dequeue(obj);

        if(obj == nullptr)
            continue;

        CritScope ss(&crt_);

        auto it = objects_.find(obj);

        if(it != objects_.end())
        {
            if(it->second)
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
    }
}

