#ifndef REMBRANDT_SRC_NETWORK_UCX_MEMORY_REGION_H_
#define REMBRANDT_SRC_NETWORK_UCX_MEMORY_REGION_H_

#include "ucp/api/ucp.h"
#include "context.h"

namespace UCP {
class MemoryRegion {
 public:
  MemoryRegion(Context &context);
  ~MemoryRegion();
  MemoryRegion(const MemoryRegion &) = delete;
  MemoryRegion &operator=(const MemoryRegion &) = delete;
  void *region_;
  ucp_mem_h ucp_mem_;
  void Pack(void **rkey_buffer_p, size_t *size_p);
 private:
  Context &context_;
};
}

#endif //REMBRANDT_SRC_NETWORK_UCX_MEMORY_REGION_H_
