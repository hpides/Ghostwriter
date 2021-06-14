#ifndef REMBRANDT_SRC_PRODUCER_PRODUCER_H_
#define REMBRANDT_SRC_PRODUCER_PRODUCER_H_

#include <memory>
#include <rembrandt/network/message.h>
#include "rembrandt/producer/producer_config.h"
#include <rembrandt/utils.h>
class Producer {
 public:
  virtual void Send(const TopicPartition &topic_partition, std::unique_ptr<Message> message) = 0;
  virtual void Send(const TopicPartition &topic_partition, std::unique_ptr<Message> message, uint64_t (&latencies)[4]) = 0;
};

#endif //REMBRANDT_SRC_PRODUCER_PRODUCER_H_
