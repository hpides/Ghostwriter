#ifndef REMBRANDT_SRC_STORAGE_SEGMENT_H_
#define REMBRANDT_SRC_STORAGE_SEGMENT_H_

#include "segment_header.h"

class Segment {
 public:
  Segment();
  Segment(void *location, uint64_t segment_size);
  ~Segment();
  Segment(const Segment &other) = delete;
  Segment(Segment &&other) noexcept;
  Segment &operator=(const Segment &other) = delete;
  Segment &operator=(Segment &&other) noexcept;
  bool Allocate(int32_t topic_id, int32_t partition_id, int32_t segment_id);
  bool Free();
  bool IsFree();
  void *GetMemoryLocation();
  int32_t GetTopicId();
  int32_t GetPartitionId();
  int32_t GetSegmentId();
  static uint64_t GetOffsetOfLastCommittedOffset();
  static uint64_t GetOffsetOfWriteOffset();
  static uint64_t GetDataOffset();
  uint64_t GetLastCommittedOffset();
  void SetLastCommittedOffset(uint64_t last_committed_offset);
  uint64_t GetSize();
 private:
  SegmentHeader *segment_header_;
  void *memory_location_;
  static void ResetHeader(SegmentHeader &segment_header);
};

#endif //REMBRANDT_SRC_STORAGE_SEGMENT_H_
