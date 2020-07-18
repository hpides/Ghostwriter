#ifndef REMBRANDT_SRC_STORAGE_VOLATILE_STORAGE_REGION_H_
#define REMBRANDT_SRC_STORAGE_VOLATILE_STORAGE_REGION_H_

#include <memory>
#include "storage_region.h"

class VolatileStorageRegion : public StorageRegion {

 public:
  VolatileStorageRegion() = delete;
  VolatileStorageRegion(size_t size, size_t alignment);
  ~VolatileStorageRegion() override = default;
  void *GetLocation() const override;
  uint64_t GetSize() const override;
  void SetSegmentSize(uint64_t segment_size) override;
 private:
  std::unique_ptr<uint8_t> location_;
  uint64_t size_;
  uint64_t segment_size_;
};

#endif //REMBRANDT_SRC_STORAGE_VOLATILE_STORAGE_REGION_H_
