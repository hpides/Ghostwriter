#include <cstring>
#include "rembrandt/storage/volatile_storage_region.h"

VolatileStorageRegion::VolatileStorageRegion(uint64_t size, uint64_t alignment) : size_(size) {
  if (size % alignment != 0) {
    throw std::invalid_argument("size must be multiple of alignment.");
  }
  location_ = std::unique_ptr<uint8_t>((uint8_t *) aligned_alloc(alignment, size));
  memset(location_.get(), 0, size_);
}

void *VolatileStorageRegion::GetLocation() const { return (void *) location_.get(); }

uint64_t VolatileStorageRegion::GetSize() const { return size_; }

uint64_t VolatileStorageRegion::GetSegmentSize() const {
  return segment_size_;
}

void VolatileStorageRegion::SetSegmentSize(uint64_t segment_size) {
  segment_size_ = segment_size;
}