#ifndef REMBRANDT_SRC_CONSUMER_CONSUMER_SEGMENT_INFO_H_
#define REMBRANDT_SRC_CONSUMER_CONSUMER_SEGMENT_INFO_H_

struct ConsumerSegmentInfo {
  uint32_t topic_id;
  uint32_t partition_id;
  uint32_t segment_id;
  uint64_t read_offset;
  uint64_t commit_offset;
  bool is_committable;
  ConsumerSegmentInfo(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id)
      : topic_id(topic_id),
        partition_id(partition_id),
        segment_id(segment_id),
        read_offset(0),
        commit_offset(0),
        is_committable(true) {}
};

#endif //REMBRANDT_SRC_CONSUMER_CONSUMER_SEGMENT_INFO_H_
