#ifndef REMBRANDT_SRC_CONSUMER_CONSUMER_H_
#define REMBRANDT_SRC_CONSUMER_CONSUMER_H_

#include "rembrandt/network/message.h"
#include <rembrandt/utils.h>

class Consumer {
 public:
  virtual std::unique_ptr<Message> Receive(uint32_t topic_id, uint32_t partition_id, std::unique_ptr<Message> message) = 0;
};

#endif //REMBRANDT_SRC_CONSUMER_CONSUMER_H_
