#include "rembrandt/consumer/direct_consumer.h"

DirectConsumer::DirectConsumer(Receiver &receiver, ConsumerConfig &config) : receiver_(receiver), config_(config) {}

std::unique_ptr<Message> DirectConsumer::Receive(uint32_t topic_id,
                                                 uint32_t partition_id,
                                                 std::unique_ptr<Message> message) {
  uint64_t offset = GetNextOffset(topic_id, partition_id);
  consumer_segment_info_->read_offset += message->GetSize();
  return receiver_.Receive(std::move(message), offset);
}

uint64_t DirectConsumer::GetNextOffset(uint32_t topic_id, uint32_t partition_id) {
  if (consumer_segment_info_ == nullptr) {
    consumer_segment_info_ = receiver_.FetchSegmentInfo(topic_id, partition_id, 1);
  } else if (consumer_segment_info_->read_offset == consumer_segment_info_->commit_offset) {
    if (consumer_segment_info_->is_committable) {
      receiver_.UpdateSegmentInfo(*consumer_segment_info_);
    } else {
      consumer_segment_info_ =
          receiver_.FetchSegmentInfo(topic_id, partition_id, consumer_segment_info_->segment_id + 1);
    }
  }
  return consumer_segment_info_->read_offset;
}
