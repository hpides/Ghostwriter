#ifndef REMBRANDT_SRC_BROKER_PARTITION_H_
#define REMBRANDT_SRC_BROKER_PARTITION_H_

#include <memory>
#include "logical_segment.h"

class Partition {
 public:
  enum class Mode {EXCLUSIVE, CONCURRENT};
  Partition(PartitionIdentifier id, Mode mode);
  void Append(std::unique_ptr<LogicalSegment> logical_segment);
  LogicalSegment *GetSegment(uint64_t logical_offset) const;
  LogicalSegment *GetSegmentById(uint32_t segment_id) const;
  bool IsEmpty() const;
  LogicalSegment &GetLatest() const;
  uint64_t GetWriteOffset() const;
  uint64_t GetCommitOffset() const;
  Mode GetMode() const;
  bool IsExclusive() const;
  bool IsConcurrent() const;
 private:
  PartitionIdentifier partition_identifier_;
  Mode mode_;
  std::vector<std::unique_ptr<LogicalSegment>> segments_;
};

#endif //REMBRANDT_SRC_BROKER_PARTITION_H_
