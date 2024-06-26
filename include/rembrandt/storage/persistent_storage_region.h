#ifndef REMBRANDT_SRC_STORAGE_PERSISTENT_STORAGE_REGION_H_
#define REMBRANDT_SRC_STORAGE_PERSISTENT_STORAGE_REGION_H_

#include <rembrandt/storage/storage_region.h>
#include <cstdlib>
#include <memory>
#include <libpmemobj++/pool.hpp>

struct StorageRegionHeader {
  uint64_t region_size_;
  uint64_t segment_size_;
};

class PersistentStorageRegion : public StorageRegion {
 public:
  PersistentStorageRegion(size_t size, size_t alignment, std::string device);
  ~PersistentStorageRegion() override;
  void *GetLocation() const override;
  uint64_t GetSize() const override;
  uint64_t GetSegmentSize() const override;
  void SetSegmentSize(uint64_t segment_size) override;
 private:
  void *location_;
  uint64_t alignment_;
  int fd_;
  StorageRegionHeader *GetHeader() const;
};

#endif //REMBRANDT_SRC_STORAGE_PERSISTENT_STORAGE_REGION_H_