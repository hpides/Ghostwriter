#ifndef REMBRANDT_SRC_PRODUCER_ASYNC_DIRECT_PRODUCER_H_
#define REMBRANDT_SRC_PRODUCER_ASYNC_DIRECT_PRODUCER_H_

#include "rembrandt/producer/producer_config.h"
class AsyncDirectProducer : public Producer {
 public:
  virtual void Send(TopicPartition topic_partition, void *buffer, size_t length) = 0;
  virtual void Start() = 0;
  virtual void Stop() = 0;
};

#endif //REMBRANDT_SRC_PRODUCER_ASYNC_DIRECT_PRODUCER_H_
