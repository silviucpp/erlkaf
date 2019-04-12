#ifndef ERLKAF_C_SRC_QUEUEMANAGER_H_
#define ERLKAF_C_SRC_QUEUEMANAGER_H_

#include "macros.h"
#include "critical_section.h"

#include <set>

typedef struct rd_kafka_s rd_kafka_t;
typedef struct rd_kafka_queue_s rd_kafka_queue_t;

class QueueManager
{
public:

    explicit QueueManager(rd_kafka_t *rk);
    ~QueueManager();

    void add(rd_kafka_queue_t* queue);
    bool remove(rd_kafka_queue_t* queue);
    void clear_all();

private:

    DISALLOW_COPY_AND_ASSIGN(QueueManager);

    CriticalSection crt_;
    rd_kafka_t* rk_;
    std::set<rd_kafka_queue_t*> queues_;
};

#endif
