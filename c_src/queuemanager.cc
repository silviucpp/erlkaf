#include "queuemanager.h"
#include "rdkafka.h"

QueueManager::QueueManager(rd_kafka_t *rk) : rk_(rk) { }

QueueManager::~QueueManager()
{
    ASSERT(queues_.empty());
}

void QueueManager::add(rd_kafka_queue_t* queue)
{
    CritScope ss(&crt_);
    ASSERT(queues_.find(queue) == queues_.end());

    // remove the queue forwarding on the main queue.
    rd_kafka_queue_forward(queue, NULL);
    queues_.insert(queue);
}

bool QueueManager::remove(rd_kafka_queue_t* queue)
{
    CritScope ss(&crt_);
    auto it = queues_.find(queue);

    if(it == queues_.end())
        return false;

    // forward the queue back to the main queue
    rd_kafka_queue_t* main_queue = rd_kafka_queue_get_consumer(rk_);
    rd_kafka_queue_forward(*it, main_queue);
    rd_kafka_queue_destroy(main_queue);
    rd_kafka_queue_destroy(*it);
    queues_.erase(it);
    return true;
}

void QueueManager::clear_all()
{
    CritScope ss(&crt_);

    // forwards all queues back on the main queue

    for(auto it = queues_.begin(); it != queues_.end(); ++it)
    {
        rd_kafka_queue_t* main_queue = rd_kafka_queue_get_consumer(rk_);
        rd_kafka_queue_forward(*it, main_queue);
        rd_kafka_queue_destroy(main_queue);
    }

    queues_.clear();
}
