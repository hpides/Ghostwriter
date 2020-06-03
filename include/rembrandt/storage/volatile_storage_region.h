#ifndef REMBRANDT_SRC_STORAGE_VOLATILE_STORAGE_REGION_H_
#define REMBRANDT_SRC_STORAGE_VOLATILE_STORAGE_REGION_H_

#include <memory>
#include "storage_region.h"

class VolatileStorageRegion : public StorageRegion {

 public:
  VolatileStorageRegion() = delete;
  VolatileStorageRegion(size_t size, size_t alignment);
  ~VolatileStorageRegion() override = default;
  void * GetLocation() const override;
  size_t GetSize() const override;
 private:
  std::unique_ptr<uint8_t> location_;
  size_t size_;
};

#endif //REMBRANDT_SRC_STORAGE_VOLATILE_STORAGE_REGION_H_
