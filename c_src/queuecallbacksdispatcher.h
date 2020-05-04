#ifndef C_SRC_QUEUE_CALLBACKS_DISPATCHER_H_
#define C_SRC_QUEUE_CALLBACKS_DISPATCHER_H_

#include "macros.h"
#include "critical_section.h"
#include <concurrentqueue/blockingconcurrentqueue.h>

#include <unordered_map>
#include <thread>

typedef struct rd_kafka_s rd_kafka_t;

class QueueCallbacksDispatcher
{
public:

    QueueCallbacksDispatcher();
    ~QueueCallbacksDispatcher();

    void watch(rd_kafka_t* instance, bool is_consumer);
    bool remove(rd_kafka_t* instance);
    void signal(rd_kafka_t* instance);

private:

    void process_callbacks();

    CriticalSection crt_;
    std::thread thread_callbacks_;

    bool running_;
    moodycamel::BlockingConcurrentQueue<rd_kafka_t*> events_;
    std::unordered_map<rd_kafka_t*, bool> objects_;

    DISALLOW_COPY_AND_ASSIGN(QueueCallbacksDispatcher);
};

#endif  // C_SRC_QUEUE_CALLBACKS_DISPATCHER_H_
