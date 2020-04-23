#include "../../include/rembrandt/storage/segment.h"

#include <new>
#include <cstdlib>

Segment::Segment() {
  segment_header_ = new SegmentHeader();
  memory_location_ = (void *) segment_header_;
  segment_header_->segment_size_ = sizeof(*segment_header_);
  // TODO: Deduplicate segment header initialization while maintaining
  //  guaranteed memory layout
  ResetHeader(*segment_header_);
}

Segment::Segment(void *location, uint64_t segment_size) {
  memory_location_ = location;
  segment_header_ = new(location) SegmentHeader();
  segment_header_->segment_size_ = segment_size;
  ResetHeader(*segment_header_);
}

Segment::~Segment() {
  free(memory_location_);
}

Segment::Segment(Segment &&other) noexcept : segment_header_(other.segment_header_),
                                             memory_location_(other.memory_location_) {
  auto *segment_header = new SegmentHeader();
  other.memory_location_ = (void *) segment_header;
  other.segment_header_ = segment_header;
  other.segment_header_->segment_size_ = sizeof(SegmentHeader);
  ResetHeader(*other.segment_header_);
}

Segment &Segment::operator=(Segment &&other) noexcept {
  std::swap(memory_location_, other.memory_location_);
  std::swap(segment_header_, other.segment_header_);
  return *this;
}

bool Segment::Allocate(int32_t topic_id,
                       int32_t partition_id,
                       int32_t segment_id) {
  if (!IsFree() || topic_id < 0 || partition_id < 0 || segment_id < 0) {
    return false;
  }
  segment_header_->free_ = false;
  segment_header_->topic_id_ = topic_id;
  segment_header_->partition_id_ = partition_id;
  segment_header_->segment_id_ = segment_id;
}

bool Segment::Free() {
  if (IsFree()) {
    return false;
  }
  ResetHeader(*segment_header_);
  return true;
}

bool Segment::IsFree() {
  return segment_header_->free_;
}

void *Segment::GetMemoryLocation() {
  return memory_location_;
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

uint64_t Segment::GetOffsetOfLastCommittedOffset() {
  return offsetof(SegmentHeader, committed_offset_);
}

uint64_t Segment::GetLastCommittedOffset() {
  return segment_header_->committed_offset_;
}

void Segment::SetLastCommittedOffset(uint64_t last_committed_offset) {
  segment_header_->committed_offset_ = last_committed_offset;
}

uint64_t Segment::GetSize() {
  return segment_header_->segment_size_;
}

void Segment::ResetHeader(SegmentHeader &segment_header) {
  segment_header.free_ = true;
  segment_header.topic_id_ = -1;
  segment_header.partition_id_ = -1;
  segment_header.segment_id_ = -1;
  segment_header.committed_offset_ = GetDataOffset();
}