#include "rembrandt/consumer/direct_consumer.h"

DirectConsumer::DirectConsumer(Receiver &receiver, ConsumerConfig &config) : receiver_(receiver), config_(config) {}

std::unique_ptr<Message> DirectConsumer::Receive(uint32_t topic_id,
                                                 uint32_t partition_id,
                                                 std::unique_ptr<Message> message) {
  uint64_t offset = AdvanceReadOffset(topic_id, partition_id, message->GetSize());
  return receiver_.Receive(std::move(message), offset);
}

uint64_t DirectConsumer::AdvanceReadOffset(uint32_t topic_id, uint32_t partition_id, uint64_t message_size) {
  uint64_t offset = GetNextOffset(topic_id, partition_id);
  read_segment_->read_offset += message_size;
  return offset;
}

uint64_t DirectConsumer::GetNextOffset(uint32_t topic_id, uint32_t partition_id) {
  if (read_segment_ == nullptr) {
    read_segment_ = receiver_.FetchReadSegment(topic_id, partition_id, 1);
  } else if (read_segment_->read_offset == read_segment_->commit_offset) {
    if (read_segment_->is_committable) {
      receiver_.UpdateReadSegment(*read_segment_);
    } else {
      read_segment_ = receiver_.FetchReadSegment(topic_id, partition_id, read_segment_->segment_id + 1);
    }
  }
  return read_segment_->read_offset;
}
