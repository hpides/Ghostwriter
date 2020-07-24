#include "rembrandt/storage/storage_manager.h"

StorageManager::StorageManager(std::unique_ptr<StorageRegion> storage_region, StorageNodeConfig config) :
    storage_region_(std::move(storage_region)), segments_(), free_segments_(), allocated_segments_() {
  storage_region_->SetSegmentSize(config.segment_size);
  char *segment_location = static_cast<char *>(storage_region_->GetLocation());
  uint64_t padding = alignof(SegmentHeader) - ((uintptr_t) segment_location % alignof(SegmentHeader));
  char *aligned_location = (char *) segment_location + padding;
  assert((((uintptr_t) aligned_location) % alignof(SegmentHeader)) == 0);
  uint64_t occupied = 0;
  while (occupied + config.segment_size <= storage_region_->GetSize() - padding) {
    assert(occupied % alignof(SegmentHeader) == 0);
    void *location = (uint8_t *) aligned_location + occupied;
    std::unique_ptr<Segment> segment = std::make_unique<Segment>(location, config.segment_size);
    free_segments_.push(segment.get());
    segments_.push_back(std::move(segment));
    occupied += config.segment_size;
  }
}

Segment *StorageManager::Allocate(uint32_t topic_id,
                                  uint32_t partition_id,
                                  uint32_t segment_id,
                                  uint64_t start_offset) {
  // TODO: Multithreading
  if (!HasFreeSegment()) return nullptr;

  Segment *segment = free_segments_.front();
  free_segments_.pop();
  segment->Allocate(topic_id, partition_id, segment_id, start_offset);
  SegmentIdentifier segment_identifier{topic_id, partition_id, segment_id};
  allocated_segments_[segment_identifier] = segment;
  return segment;
}

uint64_t StorageManager::GetOffset(void *location) const {
  return (uintptr_t) location - (uintptr_t) storage_region_->GetLocation();
}

bool StorageManager::Free(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id) {
  const SegmentIdentifier id{topic_id, partition_id, segment_id};
  auto it = allocated_segments_.find(id);
  if (it == allocated_segments_.end()) return false;

  Segment *segment = it->second;
  bool freed = segment->Free();
  assert(freed);
  allocated_segments_.erase(id);
  free_segments_.push(segment);
  return freed;
}

bool StorageManager::HasFreeSegment() const {
  return !free_segments_.empty();
}

uint64_t StorageManager::GetSegmentSize() const {
  return storage_region_->GetSegmentSize();
}