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
  SegmentInfo();
  SegmentInfo(TopicPartition topic_partition, uint64_t size);
  uint64_t Stage(uint64_t message_size);
  bool Commit(uint64_t offset);
  bool HasSpace(uint64_t message_size);
 private:
  TopicPartition topic_partition_;
  uint64_t size_;
  uint64_t byte_offset_ = 0;
  std::map<uint64_t, CommitStates> commits;
};

#endif //REMBRANDT_SRC_BROKER_SEGMENT_INFO_H_
