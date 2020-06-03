#ifndef REMBRANDT_SRC_STORAGE_PERSISTENT_STORAGE_REGION_H_
#define REMBRANDT_SRC_STORAGE_PERSISTENT_STORAGE_REGION_H_

#include <rembrandt/storage/storage_region.h>
#include <cstdlib>
#include <memory>
#include <libpmemobj++/pool.hpp>

class PersistentStorageRegion : public StorageRegion {
 public:
  PersistentStorageRegion(size_t size, size_t alignment);
  ~PersistentStorageRegion() override;
  void * GetLocation() const override;
  size_t GetSize() const override;
 private:
  const std::string PATH = "/mnt/nvrams1/rembrandt/rembrandt.data";
  pmem::obj::pool<uint8_t> pool_;
  uint8_t *location_;
  size_t size_;
};

#endif //REMBRANDT_SRC_STORAGE_PERSISTENT_STORAGE_REGION_H_
