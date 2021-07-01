#ifndef REMBRANDT_SRC_PRODUCER_PRODUCER_H_
#define REMBRANDT_SRC_PRODUCER_PRODUCER_H_

#include <memory>
#include <rembrandt/network/message.h>
#include "rembrandt/producer/producer_config.h"
class Producer {
 public:
  virtual void Send(uint32_t topic_id, uint32_t partition_id, std::unique_ptr<Message> message) = 0;
  virtual void Send(uint32_t topic_id, uint32_t partition_id, std::unique_ptr<Message> message, uint64_t (&latencies)[4]) = 0;
};

#endif //REMBRANDT_SRC_PRODUCER_PRODUCER_H_
