#ifndef REMBRANDT_SRC_BROKER_INDEX_H_
#define REMBRANDT_SRC_BROKER_INDEX_H_

#include <memory>
#include "logical_segment.h"

class Index {
 public:
  Index(PartitionIdentifier id);
  void Append(std::unique_ptr<LogicalSegment> logical_segment);
  LogicalSegment *GetSegment(uint64_t logical_offset) const; // TODO: Return more sensible value
  LogicalSegment *GetSegmentById(uint32_t segment_id) const;
  bool IsEmpty() const;
  LogicalSegment &GetLatest() const;
  uint64_t GetWriteOffset() const;
  uint64_t GetCommitOffset() const;
 private:
  PartitionIdentifier partition_identifier_;
  std::vector<std::unique_ptr<LogicalSegment>> segments_;
};

#endif //REMBRANDT_SRC_BROKER_INDEX_H_
