#ifndef REMBRANDT_SRC_BROKER_SEGMENT_INFO_H_
#define REMBRANDT_SRC_BROKER_SEGMENT_INFO_H_

#include <cstdint>
#include <rembrandt/utils.h>

class SegmentInfo {
 public:
  SegmentInfo(SegmentIdentifier id,
              uint64_t offset,
              uint64_t size);
  uint64_t Stage(uint64_t message_size);
  bool CanCommit(uint64_t offset) const;
  bool Commit(uint64_t offset);
//  std::pair<uint64_t, uint32_t> Fetch(uint64_t last_offset, uint32_t max_length);
  bool HasSpace(uint64_t message_size) const;
  bool IsWriteable() const;
  bool HasUncommittedEntries() const;
  uint32_t GetTopicId() const;
  uint32_t GetPartitionId() const;
  uint32_t GetSegmentId() const;
  uint64_t GetDataOffset() const;
  uint64_t GetWriteOffset() const;
  uint64_t GetCommittedOffset() const;
  uint64_t GetOffsetOfCommittedOffset() const;
  uint64_t GetOffsetOfWriteOffset() const;
 private:
  const SegmentIdentifier segment_identifier_;
  const uint64_t offset_;
  const uint64_t size_;
  uint64_t committed_offset_;
  uint64_t write_offset_;
  bool writeable_;
};

#endif //REMBRANDT_SRC_BROKER_SEGMENT_INFO_H_
