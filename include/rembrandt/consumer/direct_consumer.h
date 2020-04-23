#ifndef REMBRANDT_SRC_CONSUMER_DIRECT_CONSUMER_H_
#define REMBRANDT_SRC_CONSUMER_DIRECT_CONSUMER_H_

#include "consumer.h"
#include "consumer_config.h"
#include "receiver.h"

class DirectConsumer : public Consumer {
 public:
  DirectConsumer(Receiver &receiver, ConsumerConfig &config);
// TODO: Clean interface to avoid moving messages around excessively.
  std::unique_ptr<Message> Receive(TopicPartition topic_partition,
                                   std::unique_ptr<Message> message,
                                   uint64_t offset) override;
//  std::unique_ptr<Message> Receive(TopicPartition topic_partition, std::unique_ptr<Message> message) override;
 private:
  Receiver &receiver_;
  ConsumerConfig &config_;
  std::unordered_map<TopicPartition,
                     uint64_t,
                     boost::hash<TopicPartition>> committed_offsets_;
  std::unordered_map<TopicPartition,
                     uint64_t,
                     boost::hash<TopicPartition>> read_offsets_;
  void FetchCommittedOffset(TopicPartition topic_partition);
};

#endif //REMBRANDT_SRC_CONSUMER_DIRECT_CONSUMER_H_
