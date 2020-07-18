#ifndef REMBRANDT_SRC_STORAGE_SEGMENT_H_
#define REMBRANDT_SRC_STORAGE_SEGMENT_H_

#include "segment_header.h"

class Segment {
 public:
  static constexpr uint64_t FREE_BIT = 1l << (sizeof(SegmentHeader::topic_id_) * 8 - 1);
  static constexpr uint64_t WRITEABLE_BIT= 1l << (sizeof(SegmentHeader::write_offset_) * 8 - 1);
  Segment(void *location, uint64_t segment_size);
  Segment(SegmentHeader *segment_header);
  Segment(const Segment &other) = delete;
  Segment(Segment &&other) noexcept;
  Segment &operator=(const Segment &other) = delete;
  Segment &operator=(Segment &&other) noexcept;
  bool Allocate(uint64_t topic_id, uint32_t partition_id, uint32_t segment_id);
  bool Free();
  bool IsFree();
  void *GetMemoryLocation();
  uint64_t GetTopicId();
  uint32_t GetPartitionId();
  uint32_t GetSegmentId();
  static uint64_t GetOffsetOfStartOffset();
  static uint64_t GetOffsetOfCommitOffset();
  static uint64_t GetOffsetOfWriteOffset();
  static uint64_t GetDataOffset();
  uint64_t GetCommitOffset();
  void SetCommitOffset(uint64_t commit_offset);
 private:
  SegmentHeader *segment_header_;
  static void ResetHeader(SegmentHeader &segment_header);
};

#endif //REMBRANDT_SRC_STORAGE_SEGMENT_H_
