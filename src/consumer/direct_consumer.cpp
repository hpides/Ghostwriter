#include "rembrandt/consumer/direct_consumer.h"

DirectConsumer::DirectConsumer(Receiver &receiver, ConsumerConfig &config) : receiver_(receiver), config_(config) {}

std::unique_ptr<Message> DirectConsumer::Receive(TopicPartition topic_partition,
                                                 std::unique_ptr<Message> message,
                                                 uint64_t offset) {
  return receiver_.Receive(topic_partition, std::move(message), offset);
}

