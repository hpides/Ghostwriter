#ifndef REMBRANDT_SRC_CONSUMER_CONSUMER_H_
#define REMBRANDT_SRC_CONSUMER_CONSUMER_H_

#include "../../include/rembrandt/protocol/rembrandt_protocol_generated.h"

typedef std::pair<uint64_t, uint32_t> FetchInfo;

class Consumer {
 public:
  Consumer(UCP::Context &context);
  void *Receive(TopicPartition topic_partition,
                uint64_t offset,
                size_t max_length);
 private:
  uint64_t message_counter_ = 0;
  const Rembrandt::Protocol::Fetched *Consumer::HandleFetchResponse(Message &raw_message);
};

#endif //REMBRANDT_SRC_CONSUMER_CONSUMER_H_
