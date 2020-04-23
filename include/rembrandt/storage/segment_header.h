#ifndef REMBRANDT_SRC_STORAGE_SEGMENT_HEADER_H_
#define REMBRANDT_SRC_STORAGE_SEGMENT_HEADER_H_

#include <cstdint>
struct SegmentHeader {
 public:
  uint64_t segment_size_;
  bool free_;
  int32_t topic_id_;
  int32_t partition_id_;
  int32_t segment_id_;
  uint64_t committed_offset_;
};
#endif //REMBRANDT_SRC_STORAGE_SEGMENT_HEADER_H_
