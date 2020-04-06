#include "rembrandt/broker/segment_info.h"

SegmentInfo::SegmentInfo(TopicPartition topic_partition, uint64_t segment_addr, uint64_t size) :
    topic_partition_(topic_partition),
    segment_addr_(segment_addr),
    write_offset_(segment_addr),
    committed_offset_(segment_addr),
    size_(size) {}

uint64_t SegmentInfo::Stage(uint64_t message_size) {
  if (!HasSpace(message_size)) {
    Reset();
  }
  uint64_t offset = write_offset_;
  write_offset_ += message_size;
  commits[offset] = std::pair(CommitStates::STAGED, message_size);
  return offset;
}

void SegmentInfo::Reset() {
  write_offset_ = 0;
  commits.clear();
}

std::pair<uint64_t, uint32_t> SegmentInfo::Fetch(uint64_t last_offset, uint32_t max_length) {
  if (last_offset == committed_offset_) {
    return std::pair(last_offset, 0);
  }
  uint64_t offset = last_offset + commits[last_offset].second;
  uint32_t total_length = 0;
  uint64_t current_offset = offset;
  uint32_t current_length = commits[offset].second;
  while (total_length + current_length <= max_length && current_offset <= committed_offset_) {
    total_length += current_length;
    current_offset = offset + current_length;
    current_length = commits[current_offset].second;
  }
  // TODO: Handle first offset being too large
  return std::pair(offset, total_length);
}

bool SegmentInfo::Commit(uint64_t offset) {
  if (commits[offset].first != CommitStates::TIMEOUT) {
    commits[offset].first = CommitStates::COMMITTED;
    // TODO: Add logic for out-of-order commits
    if (committed_offset_ < offset) {
      committed_offset_ = offset;
    }
    return true;
  } else {
    return false;
  }
}

bool SegmentInfo::HasSpace(uint64_t message_size) {
  return ((write_offset_ + message_size) <= size_);
}