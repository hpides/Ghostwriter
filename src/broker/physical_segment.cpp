#include "rembrandt/broker/physical_segment.h"
#include <rembrandt/storage/segment.h>

PhysicalSegment::PhysicalSegment(uint64_t location) : location_(location) {}

uint64_t PhysicalSegment::GetLocation() const {
  return location_;
}

uint64_t PhysicalSegment::GetLocationOfCommitOffset() const {
  return location_ + Segment::GetOffsetOfCommitOffset();
}

uint64_t PhysicalSegment::GetLocationOfWriteOffset() const {
  return location_ + Segment::GetOffsetOfWriteOffset();
}

uint64_t PhysicalSegment::GetLocationOfData() const {
  return location_ + Segment::GetDataOffset();
}
