#ifndef REMBRANDT_SRC_CONSUMER_CONSUMER_H_
#define REMBRANDT_SRC_CONSUMER_CONSUMER_H_

#include "rembrandt/network/message.h"
#include <rembrandt/utils.h>

class Consumer {
 public:
  virtual std::unique_ptr<Message> Receive(TopicPartition topic_partition,
                                           std::unique_ptr<Message> message,
                                           uint64_t offset) = 0;
//  virtual std::unique_ptr<Message> Receive(TopicPartition topic_partition, std::unique_ptr<Message> message) = 0;
};

#endif //REMBRANDT_SRC_CONSUMER_CONSUMER_H_
