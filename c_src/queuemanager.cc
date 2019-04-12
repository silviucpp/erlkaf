#include "queuemanager.h"
#include "rdkafka.h"

QueueManager::QueueManager(rd_kafka_t *rk) : rk_(rk)
{

}

QueueManager::~QueueManager()
{
    ASSERT(queues_.empty());
}

void QueueManager::add(int32_t partition, rd_kafka_queue_t* queue)
{
    CritScope ss(&crt_);
    ASSERT(queues_.find(partition) == queues_.end());
    rd_kafka_queue_forward(queue, NULL);
    queues_[partition] = queue;
}

bool QueueManager::remove(int32_t partition)
{
    CritScope ss(&crt_);
    auto it = queues_.find(partition);

    if(it == queues_.end())
        return false;

    rd_kafka_queue_forward(it->second, rd_kafka_queue_get_consumer(rk_));
    queues_.erase(it);
    return true;
}

void QueueManager::clear_all()
{
    CritScope ss(&crt_);
    for(auto it = queues_.begin(); it != queues_.end(); ++ it)
        rd_kafka_queue_forward(it->second, rd_kafka_queue_get_consumer(rk_));

    queues_.clear();
}
