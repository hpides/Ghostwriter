#ifndef REMBRANDT_SRC_STORAGE_SEGMENT_HEADER_H_
#define REMBRANDT_SRC_STORAGE_SEGMENT_HEADER_H_

#include <cstdint>
class SegmentHeader {
 public:
  bool free_;
  uint32_t topic_id_;
  uint32_t partition_id_;
  uint32_t segment_id_;
  uint64_t segment_size_;
  SegmentHeader() = default;
};

#endif //REMBRANDT_SRC_STORAGE_SEGMENT_HEADER_H_
