#ifndef REMBRANDT_SRC_STORAGE_STORAGE_REGION_H_
#define REMBRANDT_SRC_STORAGE_STORAGE_REGION_H_

#include <cstdlib>
#include <memory>

class StorageRegion {
 public:
  StorageRegion(size_t size, size_t alignment);
  virtual ~StorageRegion() = default;
  void *GetLocation() const;
  size_t GetSize() const;
 private:
  std::unique_ptr<uint8_t> location_;
  size_t size_;

};

#endif //REMBRANDT_SRC_STORAGE_STORAGE_REGION_H_
