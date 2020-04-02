#ifndef REMBRANDT_SRC_PRODUCER_PRODUCER_H_
#define REMBRANDT_SRC_PRODUCER_PRODUCER_H_

#include <rembrandt/network/message.h>
#include "rembrandt/producer/producer_config.h"
class Producer {
 public:
  virtual void Send(TopicPartition topic_partition, std::unique_ptr<Message> message) = 0;
};

#endif //REMBRANDT_SRC_PRODUCER_PRODUCER_H_
