#include "../../include/rembrandt/broker/logical_segment.h"

LogicalSegment::LogicalSegment(SegmentIdentifier id,
                               std::unique_ptr<PhysicalSegment> physical_segment,
                               uint64_t start_offset,
                               uint64_t size)
    : segment_identifier_(id),
      physical_segment_(std::move(physical_segment)),
      start_offset_(start_offset),
      size_(size),
      commit_offset_(start_offset),
      write_offset_(start_offset),
      committable_(true),
      writeable_(true) {}

uint64_t LogicalSegment::Stage(uint64_t size) {
  if (!HasSpace(size)) {
    throw std::runtime_error("Segment is full.");
  }
  write_offset_ += size;
  return write_offset_;
}

bool LogicalSegment::CanCommit(uint64_t offset) const {
  return IsCommittable() && commit_offset_ < offset && write_offset_ >= offset;
}

bool LogicalSegment::Commit(uint64_t offset) {
  if (CanCommit(offset)) {
    commit_offset_ = offset;
    return true;
  }
  return false;
}

bool LogicalSegment::HasSpace(uint64_t size) const {
  return (write_offset_ + size - start_offset_) <= size_;
}

bool LogicalSegment::IsCommittable() const {
  return committable_;
}

bool LogicalSegment::IsWriteable() const {
  return writeable_;
}

void LogicalSegment::CloseCommits() {
  committable_ = false;
}

void LogicalSegment::CloseWrites() {
  writeable_ = false;
}

uint32_t LogicalSegment::GetTopicId() const {
  return segment_identifier_.topic_id;
}

uint32_t LogicalSegment::GetPartitionId() const {
  return segment_identifier_.partition_id;
}

uint32_t LogicalSegment::GetSegmentId() const {
  return segment_identifier_.segment_id;
}

PhysicalSegment &LogicalSegment::GetPhysicalSegment() {
  return *physical_segment_;
}

uint64_t LogicalSegment::GetStartOffset() const {
  return start_offset_;
}

uint64_t LogicalSegment::GetWriteOffset() const {
  return write_offset_;
}

uint64_t LogicalSegment::GetCommitOffset() const {
  return commit_offset_;
}

bool LogicalSegment::BelongsTo(const PartitionIdentifier &partition) const {
  return segment_identifier_.topic_id == partition.topic_id
      && segment_identifier_.partition_id == partition.partition_id;
}

uint64_t LogicalSegment::GetOffsetInSegment(uint64_t logical_offset) const {
  return logical_offset - GetStartOffset();
}