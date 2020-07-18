#ifndef REMBRANDT_SRC_CONSUMER_READ_SEGMENT_H_
#define REMBRANDT_SRC_CONSUMER_READ_SEGMENT_H_

struct ReadSegment {
  uint32_t topic_id;
  uint32_t partition_id;
  uint64_t logical_offset;
  uint64_t commit_offset;
  uint64_t remote_location;
  ReadSegment(uint32_t topic_id,
              uint32_t partition_id,
              uint64_t logical_offset,
              uint64_t commit_offset,
              uint64_t remote_location)
      : topic_id(topic_id),
        partition_id(partition_id),
        logical_offset(logical_offset),
        commit_offset(commit_offset),
        remote_location(remote_location) {}
};

#endif //REMBRANDT_SRC_CONSUMER_READ_SEGMENT_H_
