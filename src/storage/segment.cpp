#include "../../include/rembrandt/storage/segment.h"

#include <new>
#include <cstdlib>

Segment::Segment(void *location, uint64_t segment_size) {
  segment_header_ = reinterpret_cast<SegmentHeader *>(location);
  segment_header_->segment_size_ = segment_size;
  Segment::ResetHeader(*segment_header_);
}

Segment::Segment(SegmentHeader *segment_header) {
  segment_header_ = segment_header;
}

Segment::Segment(Segment &&other) noexcept : segment_header_(other.segment_header_) {
  other.segment_header_ = nullptr;
}

Segment &Segment::operator=(Segment &&other) noexcept {
  std::swap(segment_header_, other.segment_header_);
  return *this;
}

bool Segment::Allocate(int32_t topic_id,
                       int32_t partition_id,
                       int32_t segment_id) {
  if (!IsFree() || topic_id < 0 || partition_id < 0 || segment_id < 0) {
    return false;
  }
  segment_header_->segment_size_ |= FREE_BIT;
  segment_header_->topic_id_ = topic_id;
  segment_header_->partition_id_ = partition_id;
  segment_header_->segment_id_ = segment_id;
  return true;
}

bool Segment::Free() {
  if (IsFree()) {
    return false;
  }
  Segment::ResetHeader(*segment_header_);
  return true;
}

bool Segment::IsFree() {
  return (segment_header_->segment_size_ & FREE_BIT) != 0;
}

void *Segment::GetMemoryLocation() {
  return static_cast<void *>(segment_header_);
}

int32_t Segment::GetTopicId() {
  return segment_header_->topic_id_;
}

int32_t Segment::GetPartitionId() {
  return segment_header_->partition_id_;
}

int32_t Segment::GetSegmentId() {
  return segment_header_->segment_id_;
}

uint64_t Segment::GetDataOffset() {
  return sizeof(SegmentHeader);
}

uint64_t Segment::GetOffsetOfCommitOffset() {
  return offsetof(SegmentHeader, commit_offset_);
}

uint64_t Segment::GetOffsetOfWriteOffset() {
  return offsetof(SegmentHeader, write_offset_);
}

uint64_t Segment::GetCommitOffset() {
  return segment_header_->commit_offset_;
}

void Segment::SetCommitOffset(uint64_t commit_offset) {
  segment_header_->commit_offset_ = commit_offset;
}

uint64_t Segment::GetSize() {
  return segment_header_->segment_size_ & ~FREE_BIT;
}

void Segment::ResetHeader(SegmentHeader &segment_header) {
  segment_header.segment_size_ |= FREE_BIT;
  segment_header.topic_id_ = -1;
  segment_header.partition_id_ = -1;
  segment_header.segment_id_ = -1;
  segment_header.commit_offset_ = Segment::GetDataOffset() | Segment::COMMITTABLE_BIT;
  segment_header.write_offset_ = Segment::GetDataOffset() | Segment::WRITEABLE_BIT;
}