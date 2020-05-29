#ifndef REMBRANDT_SRC_CONSUMER_READ_SEGMENT_H_
#define REMBRANDT_SRC_CONSUMER_READ_SEGMENT_H_

struct ReadSegment {
  uint32_t topic_id;
  uint32_t partition_id;
  uint32_t segment_id;
  uint64_t start_offset;
  uint64_t read_offset;
  uint64_t commit_offset;
  bool is_committable;
  ReadSegment(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id)
      : topic_id(topic_id),
        partition_id(partition_id),
        segment_id(segment_id),
        start_offset(0),
        read_offset(0),
        commit_offset(0),
        is_committable(true) {}
  ReadSegment(uint32_t topic_id,
              uint32_t partition_id,
              uint32_t segment_id,
              uint64_t start_offset,
              uint64_t commit_offset,
              bool is_committable)
      : topic_id(topic_id),
        partition_id(partition_id),
        segment_id(segment_id),
        start_offset(start_offset),
        read_offset(start_offset),
        commit_offset(commit_offset),
        is_committable(is_committable) {}
};

#endif //REMBRANDT_SRC_CONSUMER_READ_SEGMENT_H_
