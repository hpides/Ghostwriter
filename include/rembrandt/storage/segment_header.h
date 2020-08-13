#ifndef REMBRANDT_SRC_STORAGE_SEGMENT_HEADER_H_
#define REMBRANDT_SRC_STORAGE_SEGMENT_HEADER_H_

#include <cstdint>
struct alignas(256) SegmentHeader {
 public:
  // First bit indicates whether the segment is free to be allocated
  // The remaining 63 bits represent the segment id
  uint64_t topic_id_;
  uint32_t partition_id_;
  uint32_t segment_id_;
  uint64_t start_offset_;
  // First bit indicates whether further values may be written to segment
  uint64_t write_offset_;
  uint64_t commit_offset_;
};
#endif //REMBRANDT_SRC_STORAGE_SEGMENT_HEADER_H_
