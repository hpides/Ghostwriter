#include "../../include/rembrandt/broker/partition.h"

Partition::Partition(PartitionIdentifier id, Mode mode) : partition_identifier_(id), mode_(mode) {}

void Partition::Append(std::unique_ptr<LogicalSegment> logical_segment) {
  assert(logical_segment->BelongsTo(partition_identifier_)); // The segment does not belong to the partition of the index.
  assert(segments_.empty() || segments_.back()->GetWriteOffset() == logical_segment->GetStartOffset());

  segments_.push_back(std::move(logical_segment));
}

LogicalSegment *Partition::GetSegment(uint64_t logical_offset) const {
  if (IsEmpty()
      || segments_.front()->GetStartOffset() > logical_offset
      || segments_.back()->GetWriteOffset() < logical_offset)
    return nullptr;

  size_t lo = 0;
  size_t hi = segments_.size() - 1;
  size_t middle;
  while (lo < hi) {
    middle = lo + (hi - lo) / 2;
    if (segments_[middle]->GetStartOffset() > logical_offset) {
      hi = middle - 1;
    } else if (segments_[middle]->GetWriteOffset() > logical_offset) {
      hi = middle;
    } else {
      lo = middle + 1;
    }
  }
  return segments_[lo].get();
}

LogicalSegment *Partition::GetSegmentById(uint32_t segment_id) const {
  if (segments_.size() < segment_id) return nullptr;
  return segments_.at(segment_id - 1).get();
}

bool Partition::IsEmpty() const {
  return segments_.empty();
}

LogicalSegment &Partition::GetLatest() const {
  return *(segments_.back());
}

uint64_t Partition::GetCommitOffset() const {
  if (IsEmpty()) return 0;
  return segments_.back()->GetCommitOffset();
}

uint64_t Partition::GetWriteOffset() const {
  if (IsEmpty()) return 0;
  return segments_.back()->GetWriteOffset();
}

Partition::Mode Partition::GetMode() const {
  return mode_;
}

bool Partition::IsExclusive() const {
  return mode_ == Partition::Mode::EXCLUSIVE;
}

bool Partition::IsConcurrent() const {
  return mode_ == Partition::Mode::CONCURRENT;
}