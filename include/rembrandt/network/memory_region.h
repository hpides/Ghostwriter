#ifndef REMBRANDT_SRC_NETWORK_MEMORY_REGION_H_
#define REMBRANDT_SRC_NETWORK_MEMORY_REGION_H_

#include <ucp/api/ucp.h>

class MemoryRegion {
 public:
  MemoryRegion(ucp_context_h &ucp_context);
  ~MemoryRegion();
  ucp_context_h ucp_context_;
  ucp_mem_h ucp_mem_;
  void Pack(void ** rkey_buffer_p, size_t *size_p);
};

#endif //REMBRANDT_SRC_NETWORK_MEMORY_REGION_H_
