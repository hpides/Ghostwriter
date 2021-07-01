#ifndef REMBRANDT_SRC_CONSUMER_DIRECT_CONSUMER_H_
#define REMBRANDT_SRC_CONSUMER_DIRECT_CONSUMER_H_

#include "consumer.h"
#include "consumer_config.h"
#include "read_segment.h"
#include "receiver.h"

class DirectConsumer : public Consumer {
public:
  static std::unique_ptr<DirectConsumer> Create(ConsumerConfig config, UCP::Context &context);
  std::unique_ptr<Message> Receive(uint32_t topic_id, uint32_t partition_id,
                                   std::unique_ptr<Message> message) override;

private:
  DirectConsumer(std::unique_ptr<Receiver> receiver_p, ConsumerConfig config);
  std::unique_ptr<Receiver> receiver_p_;
  std::unique_ptr<ReadSegment> read_segment_;
  ConsumerConfig config_;
  uint64_t GetNextOffset(uint32_t topic_id, uint32_t partition_id);
  uint64_t AdvanceReadOffset(uint32_t topic_id, uint32_t partition_id,
                             uint64_t message_size);
  std::unique_ptr<Message> ExclusiveReceive(uint32_t topic_id,
                                            uint32_t partition_id,
                                            std::unique_ptr<Message> message);
  std::unique_ptr<Message> ConcurrentReceive(uint32_t topic_id,
                                             uint32_t partition_id,
                                             std::unique_ptr<Message> message);
};

#endif // REMBRANDT_SRC_CONSUMER_DIRECT_CONSUMER_H_
