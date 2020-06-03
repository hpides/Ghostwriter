#ifndef REMBRANDT_SRC_STORAGE_STORAGE_REGION_H_
#define REMBRANDT_SRC_STORAGE_STORAGE_REGION_H_

#include <cstdlib>

class StorageRegion {
 public:
  virtual ~StorageRegion() = 0;
  virtual void *GetLocation() const = 0;
  virtual size_t GetSize() const = 0;
};

inline StorageRegion::~StorageRegion() {}

#endif //REMBRANDT_SRC_STORAGE_STORAGE_REGION_H_
