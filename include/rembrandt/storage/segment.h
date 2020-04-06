#ifndef REMBRANDT_SRC_STORAGE_SEGMENT_H_
#define REMBRANDT_SRC_STORAGE_SEGMENT_H_

#include "segment_header.h"

class Segment {
 public:
  Segment(void *location, uint64_t segment_size);
  void Allocate(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id);
  void Free();
  bool IsFree();
  void *GetMemoryLocation();
  uint32_t GetTopicId();
  uint32_t GetPartitionId();
  uint32_t GetSegmentId();
  int64_t GetDataOffset();
  uint64_t GetSize();
 private:
  SegmentHeader *segment_header_;
  void *memory_location_;
};

#endif //REMBRANDT_SRC_STORAGE_SEGMENT_H_
