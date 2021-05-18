#include <unistd.h>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmem.h>
#include <fcntl.h>
#include <sys/mman.h>
#include "rembrandt/storage/persistent_storage_region.h"

PersistentStorageRegion::PersistentStorageRegion(uint64_t size, uint64_t alignment) {
  int sds_write_value = 0;
  pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

  if ((fd_ = open(PATH.c_str(), O_RDWR, 0666)) < 0) {
    perror("open");
    exit(1);
  }

  const auto protection = PROT_WRITE | PROT_READ;
  const auto args = MAP_SHARED;
  location_ = mmap(nullptr, size, protection, args, fd_, 0);
  if (location_ == nullptr) {
    perror("mmap");
  }
  StorageRegionHeader *storage_region_header_p = static_cast<StorageRegionHeader *>(location_);
  storage_region_header_p->region_size_ = size;
  pmem_persist(location_, sizeof(StorageRegionHeader));
}

PersistentStorageRegion::~PersistentStorageRegion() noexcept {
  close(fd_);
}

void *PersistentStorageRegion::GetLocation() const {
  return (char *) location_ + sizeof(StorageRegionHeader);
}

uint64_t PersistentStorageRegion::GetSize() const {
  return GetHeader()->region_size_ - sizeof(StorageRegionHeader);
}

uint64_t PersistentStorageRegion::GetSegmentSize() const {
  return GetHeader()->segment_size_;
}

void PersistentStorageRegion::SetSegmentSize(uint64_t segment_size) {
  GetHeader()->segment_size_ = segment_size;
  pmem_persist(GetHeader(), sizeof(StorageRegionHeader));
}

StorageRegionHeader * PersistentStorageRegion::GetHeader() const {
  return static_cast<StorageRegionHeader *>(location_);
}