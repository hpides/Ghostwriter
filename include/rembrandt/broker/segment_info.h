#ifndef REMBRANDT_SRC_BROKER_SEGMENT_INFO_H_
#define REMBRANDT_SRC_BROKER_SEGMENT_INFO_H_

#include <cstdint>
#include <rembrandt/utils.h>

class SegmentInfo {
 public:
  SegmentInfo(TopicPartition topic_partition,
              uint64_t offset,
              uint64_t size);
  uint64_t Stage(uint64_t message_size);
  bool CanCommit(uint64_t offset);
  bool Commit(uint64_t offset);
//  std::pair<uint64_t, uint32_t> Fetch(uint64_t last_offset, uint32_t max_length);
  bool HasSpace(uint64_t message_size);
  uint64_t GetDataOffset() const;
  uint64_t GetWriteOffset() const;
  uint64_t GetCommittedOffset() const;
  uint64_t GetOffsetOfCommittedOffset() const;
  uint64_t GetOffsetOfWriteOffset() const;
 private:
  TopicPartition topic_partition_;
  const uint64_t offset_;
  uint64_t write_offset_;
  uint64_t committed_offset_;
  uint64_t size_;
  void Reset();
};

#endif //REMBRANDT_SRC_BROKER_SEGMENT_INFO_H_
