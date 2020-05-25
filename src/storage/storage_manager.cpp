#include "rembrandt/storage/storage_manager.h"

StorageManager::StorageManager(std::unique_ptr<StorageRegion> storage_region, StorageNodeConfig config) :
    storage_region_(std::move(storage_region)), segments_(), free_segments_(), allocated_segments_() {
  assert((((uintptr_t) storage_region_->GetLocation()) % alignof(SegmentHeader)) == 0);
  uint64_t occupied = 0;
  while (occupied + config.segment_size <= storage_region_->GetSize()) {
    void *location = (uint8_t *) storage_region_->GetLocation() + occupied;
    std::unique_ptr<Segment> segment = std::make_unique<Segment>(location, config.segment_size);
    free_segments_.push(segment.get());
    segments_.push_back(std::move(segment));
    occupied += config.segment_size;
  }
}

Segment *StorageManager::Allocate(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id) {
  // TODO: Multithreading
  if (!HasFreeSegment()) return nullptr;

  Segment *segment = free_segments_.front();
  free_segments_.pop();
  bool allocated = segment->Allocate(topic_id, partition_id, segment_id);
  assert(allocated);
  SegmentIdentifier segment_identifier{topic_id, partition_id, segment_id};
  allocated_segments_[segment_identifier] = segment;
  return segment;
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

}

bool StorageManager::HasFreeSegment() const {
  return !free_segments_.empty();
}
