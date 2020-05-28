#ifndef REMBRANDT_SRC_CONSUMER_DIRECT_CONSUMER_H_
#define REMBRANDT_SRC_CONSUMER_DIRECT_CONSUMER_H_

#include "consumer.h"
#include "consumer_config.h"
#include "receiver.h"

class DirectConsumer : public Consumer {
 public:
  DirectConsumer(Receiver &receiver, ConsumerConfig &config);
// TODO: Clean interface to avoid moving messages around excessively.
  std::unique_ptr<Message> Receive(uint32_t topic_id, uint32_t partition_id, std::unique_ptr<Message> message) override;
 private:
  Receiver &receiver_;
  ConsumerConfig &config_;
  std::unique_ptr<ConsumerSegmentInfo> consumer_segment_info_;
  uint64_t GetNextOffset(uint32_t topic_id, uint32_t partition_id);
};

#endif //REMBRANDT_SRC_CONSUMER_DIRECT_CONSUMER_H_
