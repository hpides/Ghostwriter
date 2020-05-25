#ifndef REMBRANDT_SRC_NETWORK_UCX_MEMORY_REGION_H_
#define REMBRANDT_SRC_NETWORK_UCX_MEMORY_REGION_H_

#include <rembrandt/storage/storage_region.h>
#include "ucp/api/ucp.h"
#include "context.h"

namespace UCP {
class MemoryRegion {
 public:
  MemoryRegion() = delete;
  MemoryRegion(Context &context, StorageRegion &storage_region);
  ~MemoryRegion();
  MemoryRegion(const MemoryRegion &) = delete;
  MemoryRegion(MemoryRegion &&other) noexcept = delete;
  MemoryRegion &operator=(const MemoryRegion &) = delete;
  MemoryRegion &operator=(MemoryRegion &&other) noexcept = delete;
  ucp_mem_h ucp_mem_;
  void Pack(void **rkey_buffer_p, size_t *size_p);
  void * GetRegion();
  size_t GetSize();
  const std::string &GetRKey() const;
 private:
  Context &context_;
  StorageRegion &storage_region_;
  std::string rkey_;
};
}

#endif //REMBRANDT_SRC_NETWORK_UCX_MEMORY_REGION_H_
