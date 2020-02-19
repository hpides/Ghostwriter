#include "rembrandt/broker/segment_info.h"

SegmentInfo::SegmentInfo(TopicPartition topic_partition, uint64_t size) {
  topic_partition_ = topic_partition;
  size_ = size;
}

uint64_t SegmentInfo::Stage(uint64_t message_size) {
  assert(HasSpace(message_size));
  uint64_t offset = byte_offset_;
  byte_offset_ += message_size;
  commits[offset] = CommitStates::STAGED;
}

bool SegmentInfo::Commit(uint64_t offset) {
  if (commits[offset] != CommitStates::TIMEOUT) {
    commits[offset] = CommitStates::COMMITTED;
    return true;
  } else {
    return false;
  }
}

bool SegmentInfo::HasSpace(uint64_t message_size) {
  return (byte_offset_ + message_size <= size_);
}