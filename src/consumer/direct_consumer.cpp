#include "rembrandt/consumer/direct_consumer.h"

DirectConsumer::DirectConsumer(Receiver &receiver, ConsumerConfig &config) : receiver_(receiver), config_(config) {}

std::unique_ptr<Message> DirectConsumer::Receive(TopicPartition topic_partition,
                                                 std::unique_ptr<Message> message,
                                                 uint64_t offset) {
  read_offsets_[topic_partition] += message->GetSize();
  return receiver_.Receive(topic_partition, std::move(message), offset);
}

std::unique_ptr<Message> DirectConsumer::Receive(TopicPartition topic_partition, std::unique_ptr<Message> message) {
  OffsetMap::iterator it;
  if (read_offsets_.count(topic_partition) == 0) {
    FetchInitialOffsets(topic_partition);
  }
  uint64_t read_offset = read_offsets_.at(topic_partition);
  uint64_t committed_offset = committed_offsets_.at(topic_partition);
  if (read_offset == committed_offset) {
    FetchCommittedOffset(topic_partition);
    committed_offset = committed_offsets_.at(topic_partition);
  }
  while (read_offset >= committed_offset) {
    // TODO: Adjust retry delay
    usleep(1000);
    FetchCommittedOffset(topic_partition);
    committed_offset = committed_offsets_.at(topic_partition);
  }
  return Receive(topic_partition, std::move(message), read_offset);
}


void DirectConsumer::FetchCommittedOffset(TopicPartition topic_partition) {
  committed_offsets_[topic_partition] = receiver_.FetchCommittedOffset(topic_partition);
}

void DirectConsumer::FetchInitialOffsets(TopicPartition topic_partition) {
  std::pair<uint64_t, uint64_t> initial_offsets = receiver_.FetchInitialOffsets(topic_partition);
  read_offsets_[topic_partition] = initial_offsets.first;
  committed_offsets_[topic_partition] = initial_offsets.second;
}