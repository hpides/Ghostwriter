#include <unistd.h>
#include <libpmemobj++/persistent_ptr.hpp>
#include "rembrandt/storage/persistent_storage_region.h"

#define LAYOUT_NAME "foo"

PersistentStorageRegion::PersistentStorageRegion(size_t size, size_t alignment) : size_(size) {
  int sds_write_value = 0;
  pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
  if (access(PATH.c_str(), F_OK)) {
    pool_ = pmem::obj::pool<uint8_t>::create(PATH, "", size_, S_IRWXU);
  } else {
    pool_ = pmem::obj::pool<uint8_t>::open(PATH, "");
  }
  uint8_t *direct_root = pool_.root().get();
  size_t remainder = ((uintptr_t) direct_root) % alignment;
  size_ -=remainder;
  location_ = remainder ? direct_root + (alignment - remainder) : direct_root;
}

PersistentStorageRegion::~PersistentStorageRegion() {
  pool_.close();
}

void *PersistentStorageRegion::GetLocation() const {
  return location_;
}

size_t PersistentStorageRegion::GetSize() const {
  return size_;
}