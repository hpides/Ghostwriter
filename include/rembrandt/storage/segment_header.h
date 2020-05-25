#ifndef REMBRANDT_SRC_STORAGE_SEGMENT_HEADER_H_
#define REMBRANDT_SRC_STORAGE_SEGMENT_HEADER_H_

#include <cstdint>
// TODO: Optimize alignment
struct alignas(256) SegmentHeader {
 public:
  // First bit indicates whether the segment is free to be allocated
  // The remaining 63 bits indicate segment size in bytes
  int64_t segment_size_;
  int32_t topic_id_;
  int32_t partition_id_;
  int32_t segment_id_;
  // First bit indicates whether further values may be written to segment
  int64_t write_offset_;
  // First bit indicates whether further values may be committed in the future
  int64_t commit_offset_;
};
#endif //REMBRANDT_SRC_STORAGE_SEGMENT_HEADER_H_
