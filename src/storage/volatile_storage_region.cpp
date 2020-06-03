#include <cstring>
#include "rembrandt/storage/volatile_storage_region.h"

VolatileStorageRegion::VolatileStorageRegion(size_t size, size_t alignment) : size_(size) {
  if (size % alignment != 0) {
    throw std::invalid_argument("size must be multiple of alignment.");
  }
  location_ = std::unique_ptr<uint8_t>((uint8_t *) aligned_alloc(alignment, size));
  memset(location_.get(), 0, size_);
}

void *VolatileStorageRegion::GetLocation() const { return (void *) location_.get(); }

size_t VolatileStorageRegion::GetSize() const { return size_; }
