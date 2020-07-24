#include "rembrandt/consumer/direct_consumer.h"
#include <rembrandt/broker/broker_node.h>

DirectConsumer::DirectConsumer(Receiver &receiver, ConsumerConfig &config) : receiver_(receiver),
                                                                             config_(config),
                                                                             read_segment_(nullptr) {}

std::unique_ptr<Message> DirectConsumer::Receive(uint32_t topic_id,
                                                 uint32_t partition_id,
                                                 std::unique_ptr<Message> message) {
  uint64_t *flag;
  do {
    uint64_t offset = AdvanceReadOffset(topic_id, partition_id, message->GetSize());
    message = receiver_.Receive(std::move(message), offset);
    flag = (uint64_t *) (message->GetBuffer() + message->GetSize() -  sizeof(BrokerNode::COMMIT_FLAG));
  } while (*flag == BrokerNode::TIMEOUT_FLAG);
  if ( *flag != BrokerNode::COMMIT_FLAG) {
    throw std::runtime_error("Unknown flag value");
  }
  return message;
}

uint64_t DirectConsumer::AdvanceReadOffset(uint32_t topic_id, uint32_t partition_id, uint64_t message_size) {
  uint64_t offset = GetNextOffset(topic_id, partition_id);
  read_segment_->remote_location += message_size;
  read_segment_->logical_offset += message_size;
  return offset;
}

uint64_t DirectConsumer::GetNextOffset(uint32_t topic_id, uint32_t partition_id) {
  if (read_segment_ == nullptr) {
    read_segment_ = receiver_.Fetch(topic_id, partition_id, 0);
  } else {//if (read_segment_->logical_offset == read_segment_->commit_offset) {
    read_segment_ = receiver_.Fetch(topic_id, partition_id, read_segment_->logical_offset);
  }
  return read_segment_->remote_location;
}
