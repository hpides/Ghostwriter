#ifndef REMBRANDT_SRC_BROKER_SEGMENT_INFO_H_
#define REMBRANDT_SRC_BROKER_SEGMENT_INFO_H_

#include <cstdint>
#include <rembrandt/utils.h>

enum CommitStates {
  STAGED = 0,
  COMMITTED = 1,
  TIMEOUT = -1
};

class SegmentInfo {
 public:
  SegmentInfo(TopicPartition topic_partition, uint64_t segment_addr, uint64_t size);
  uint64_t Stage(uint64_t message_size);
  bool Commit(uint64_t offset);
  std::pair<uint64_t, uint32_t> Fetch(uint64_t last_offset, uint32_t max_length);
  bool HasSpace(uint64_t message_size);
 private:
  TopicPartition topic_partition_;
  uint64_t segment_addr_;
  uint64_t write_offset_;
  uint64_t committed_offset_;
  uint64_t size_;
  std::map<uint64_t, std::pair<CommitStates, uint32_t>> commits;
  void Reset();
};

#endif //REMBRANDT_SRC_BROKER_SEGMENT_INFO_H_
