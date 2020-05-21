#include "rembrandt/broker/segment_info.h"
#include "rembrandt/storage/segment.h"

SegmentInfo::SegmentInfo(TopicPartition topic_partition,
                         uint64_t offset,
                         uint64_t size) :
    topic_partition_(topic_partition),
    offset_(offset),
    size_(size),
    committed_offset_(offset + Segment::GetDataOffset()),
    write_offset_(offset + Segment::GetDataOffset()) {}

uint64_t SegmentInfo::Stage(uint64_t message_size) {
  if (!HasSpace(message_size)) {
    Reset();
  }
  write_offset_ += message_size;
  return write_offset_;
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

bool SegmentInfo::CanCommit(uint64_t offset) {
  return (committed_offset_ < offset && write_offset_ >= offset);
}

bool SegmentInfo::Commit(uint64_t offset) {
  if (CanCommit(offset)) {
    committed_offset_ = offset;
    return true;
  }
  return false;
}

bool SegmentInfo::HasSpace(uint64_t message_size) {
  return ((write_offset_ + message_size) <= size_);
}

uint64_t SegmentInfo::GetCommittedOffset() const {
  return committed_offset_;
}

uint64_t SegmentInfo::GetDataOffset() const {
  return offset_ + Segment::GetDataOffset();
}

uint64_t SegmentInfo::GetOffsetOfCommittedOffset() const {
  return offset_ + Segment::GetOffsetOfLastCommittedOffset();
}

uint64_t SegmentInfo::GetWriteOffset() const {
  return write_offset_;
}

uint64_t SegmentInfo::GetOffsetOfWriteOffset() const {
  return offset_ + Segment::GetOffsetOfWriteOffset();
}