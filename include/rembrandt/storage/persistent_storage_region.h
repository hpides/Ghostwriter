#ifndef REMBRANDT_SRC_STORAGE_PERSISTENT_STORAGE_REGION_H_
#define REMBRANDT_SRC_STORAGE_PERSISTENT_STORAGE_REGION_H_

#include <rembrandt/storage/storage_region.h>
#include <cstdlib>
class PersistentStorageRegion : public StorageRegion {
 public:
  PersistentStorageRegion(size_t size, size_t alignment);
  ~PersistentStorageRegion() = default;
};

#endif //REMBRANDT_SRC_STORAGE_PERSISTENT_STORAGE_REGION_H_
