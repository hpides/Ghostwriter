#include "rembrandt/broker/segment_info.h"

SegmentInfo::SegmentInfo(TopicPartition topic_partition,
                         uint64_t data_offset,
                         uint64_t offset_of_committed_offset,
                         uint64_t size) :
    topic_partition_(topic_partition),
    data_offset_(data_offset),
    write_offset_(data_offset),
    committed_offset_(data_offset),
    offset_of_committed_offset_(offset_of_committed_offset),
    size_(size) {}

uint64_t SegmentInfo::Stage(uint64_t message_size) {
  if (!HasSpace(message_size)) {
    Reset();
  }
  uint64_t offset = write_offset_;
  write_offset_ += message_size;
  return offset;
}

void SegmentInfo::Reset() {
  write_offset_ = 0;
  committed_offset_ = 0;
}

//std::pair<uint64_t, uint32_t> SegmentInfo::Fetch(uint64_t last_offset, uint32_t max_length) {
//  if (last_offset == committed_offset_) {
//    return std::pair(last_offset, 0);
//  }
//  uint64_t offset = last_offset + commits[last_offset].second;
//  uint32_t total_length = 0;
//  uint64_t current_offset = offset;
//  uint32_t current_length = commits[offset].second;
//  while (total_length + current_length <= max_length && current_offset <= committed_offset_) {
//    total_length += current_length;
//    current_offset = offset + current_length;
//    current_length = commits[current_offset].second;
//  }
//  // TODO: Handle first offset being too large
//  return std::pair(offset, total_length);
//}

bool SegmentInfo::Commit(uint64_t offset) {
  if (committed_offset_ < offset) {
    committed_offset_ = offset;
    return true;
  }
  return false;
}

bool SegmentInfo::HasSpace(uint64_t message_size) {
  return ((write_offset_ + message_size) <= size_);
}

uint64_t SegmentInfo::GetCommittedOffset() {
  return committed_offset_;
}

uint64_t SegmentInfo::GetDataOffset() {
  return data_offset_;
}

uint64_t SegmentInfo::GetWriteOffset() {
  return write_offset_;
}