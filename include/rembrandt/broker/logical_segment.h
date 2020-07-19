#ifndef REMBRANDT_SRC_BROKER_LOGICAL_SEGMENT_H_
#define REMBRANDT_SRC_BROKER_LOGICAL_SEGMENT_H_

#include <cstdint>
#include <rembrandt/utils.h>
#include <rembrandt/storage/storage_manager.h>
#include "physical_segment.h"

class LogicalSegment {
 public:
  LogicalSegment(SegmentIdentifier id,
                 std::unique_ptr<PhysicalSegment> physical_segment,
                 uint64_t start_offset,
                 uint64_t size);
  uint64_t Stage(uint64_t size);
  bool CanCommit(uint64_t offset) const;
  bool Commit(uint64_t offset);
  bool HasSpace(uint64_t size) const;
  bool IsWriteable() const;
  bool IsCommittable() const;
  void CloseWrites();
  void CloseCommits();
  uint32_t GetTopicId() const;
  uint32_t GetPartitionId() const;
  uint32_t GetSegmentId() const;
  PhysicalSegment &GetPhysicalSegment();
  uint64_t GetStartOffset() const;
  uint64_t GetWriteOffset() const;
  uint64_t GetCommitOffset() const;
  uint64_t GetSpace() const;
  bool BelongsTo(const PartitionIdentifier &partition) const;
  uint64_t GetOffsetInSegment(uint64_t logical_offset) const;
 private:
  const SegmentIdentifier segment_identifier_;
  const std::unique_ptr<PhysicalSegment> physical_segment_;
  const uint64_t start_offset_;
  const uint64_t size_;
  uint64_t commit_offset_;
  uint64_t write_offset_;
  bool committable_;
  bool writeable_;
};

#endif //REMBRANDT_SRC_BROKER_LOGICAL_SEGMENT_H_
