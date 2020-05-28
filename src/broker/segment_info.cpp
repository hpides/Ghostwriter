#include <rembrandt/storage/storage_manager.h>
#include "rembrandt/broker/segment_info.h"
#include "rembrandt/storage/segment.h"

SegmentInfo::SegmentInfo(SegmentIdentifier id,
                         uint64_t offset,
                         uint64_t size) :
    segment_identifier_(id),
    offset_(offset),
    size_(size),
    commit_offset_(Segment::GetDataOffset()),
    write_offset_(Segment::GetDataOffset()),
    committable_(true),
    writeable_(true) {}

uint64_t SegmentInfo::Stage(uint64_t message_size) {
  if (!HasSpace(message_size)) {
    throw std::runtime_error("Segment is full.");
  }
  write_offset_ += message_size;
  return write_offset_;
}

//std::pair<uint64_t, uint32_t> SegmentInfo::FetchRequest(uint64_t last_offset, uint32_t max_length) {
//  if (last_offset == commit_offset_) {
//    return std::pair(last_offset, 0);
//  }
//  uint64_t offset = last_offset + commits[last_offset].second;
//  uint32_t total_length = 0;
//  uint64_t current_offset = offset;
//  uint32_t current_length = commits[offset].second;
//  while (total_length + current_length <= max_length && current_offset <= commit_offset_) {
//    total_length += current_length;
//    current_offset = offset + current_length;
//    current_length = commits[current_offset].second;
//  }
//  // TODO: Handle first offset being too large
//  return std::pair(offset, total_length);
//}

bool SegmentInfo::CanCommit(uint64_t offset) const {
  return (IsCommittable() && commit_offset_ < offset && write_offset_ >= offset);
}

bool SegmentInfo::Commit(uint64_t offset) {
  if (CanCommit(offset)) {
    commit_offset_ = offset;
    return true;
  }
  return false;
}

bool SegmentInfo::HasSpace(uint64_t message_size) const {
  return ((write_offset_ + message_size) <= size_);
}

void SegmentInfo::CloseCommits() {
  committable_ = false;
}

void SegmentInfo::CloseWrites() {
  writeable_ = false;
}

bool SegmentInfo::IsCommittable() const {
  return committable_;
}

bool SegmentInfo::IsWriteable() const {
  return writeable_;
}

uint32_t SegmentInfo::GetTopicId() const { return segment_identifier_.topic_id; }

uint32_t SegmentInfo::GetPartitionId() const { return segment_identifier_.partition_id; }

uint32_t SegmentInfo::GetSegmentId() const { return segment_identifier_.segment_id; }

uint64_t SegmentInfo::GetCommitOffset() const {
  return commit_offset_;
}

uint64_t SegmentInfo::GetOffset() const {
  return offset_;
}

uint64_t SegmentInfo::GetDataOffset() const {
  return offset_ + Segment::GetDataOffset();
}

uint64_t SegmentInfo::GetOffsetOfCommitOffset() const {
  return offset_ + Segment::GetOffsetOfCommitOffset();
}

uint64_t SegmentInfo::GetWriteOffset() const {
  return write_offset_;
}

uint64_t SegmentInfo::GetOffsetOfWriteOffset() const {
  return offset_ + Segment::GetOffsetOfWriteOffset();
}