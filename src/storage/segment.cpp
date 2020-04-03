#include "../../include/rembrandt/storage/segment.h"

#include <assert.h>
#include <new>
Segment::Segment(void *location, uint64_t segment_size) {
  memory_location_ = location;
  segment_header_ = new(location) SegmentHeader();
  segment_header_->segment_size_ = segment_size;
  segment_header_->free_ = true;
}

void Segment::Allocate(uint32_t topic_id,
                       uint32_t partition_id,
                       uint32_t segment_id) {
  segment_header_->free_ = false;
  assert(topic_id != 0);
  segment_header_->topic_id_ = topic_id;
  assert(partition_id != 0);
  segment_header_->partition_id_ = partition_id;
  assert(segment_id != 0);
  segment_header_->segment_id_ = segment_id;
}

void Segment::Free() {
  segment_header_->free_ = true;
  segment_header_->topic_id_ = 0;
  segment_header_->partition_id_ = 0;
  segment_header_->segment_id_ = 0;
}

bool Segment::IsFree() {
  return segment_header_->free_;
}

void *Segment::GetMemoryLocation() {
  return memory_location_;
}

uint32_t Segment::GetTopicId() {
  return segment_header_->topic_id_;
}

uint32_t Segment::GetPartitionId() {
  return segment_header_->partition_id_;
}

uint32_t Segment::GetSegmentId() {
  return segment_header_->segment_id_;
}

int64_t Segment::GetDataOffset() {
  return ((char *) memory_location_ - (char *) memory_location_)
      + sizeof(*segment_header_);
}

uint64_t Segment::GetSize() {
  return size_;
}