#include <unistd.h>
#include <libpmemobj++/persistent_ptr.hpp>
#include <fcntl.h>
#include <sys/mman.h>
#include "rembrandt/storage/persistent_storage_region.h"

PersistentStorageRegion::PersistentStorageRegion(size_t size, size_t alignment) : size_(size) {
  int sds_write_value = 0;
  pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

  if ((fd_ = open(PATH.c_str(), O_RDWR, 0666)) < 0) {
    perror("open");
    exit(1);
  }

  const auto protection = PROT_WRITE | PROT_READ | PROT_EXEC;
  const auto args = MAP_SHARED;
  location_ = mmap(nullptr, size_, protection, args, fd_, 0);
}

PersistentStorageRegion::~PersistentStorageRegion() noexcept {
  close(fd_);
}

void *PersistentStorageRegion::GetLocation() const {
  return location_;
}

size_t PersistentStorageRegion::GetSize() const {
  return size_;
}