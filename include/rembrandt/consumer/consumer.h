#ifndef REMBRANDT_SRC_CONSUMER_CONSUMER_H_
#define REMBRANDT_SRC_CONSUMER_CONSUMER_H_

#include "rembrandt/network/ucx/client.h"

class Consumer {
 public:
  Consumer(UCP::Context &context);
  void *Receive(TopicPartition topic_partition,
                uint64_t offset,
                size_t max_length);
 private:
  UCP::Client client_;
};

#endif //REMBRANDT_SRC_CONSUMER_CONSUMER_H_
