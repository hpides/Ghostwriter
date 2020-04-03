
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <stdlib.h>
#include <rembrandt/network/ucx/memory_region.h>

using namespace UCP;

MemoryRegion::MemoryRegion(Context &context, long size) : context_(context), size_(size) {
  // TODO: FIX CONSTRUCTION
  region_ = (char *) malloc(size_);
  memset(region_, 0, size_);

  ucp_mem_map_params_t mem_map_params;
  memset(&mem_map_params, 0, sizeof(mem_map_params));

  mem_map_params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
      UCP_MEM_MAP_PARAM_FIELD_LENGTH |
      UCP_MEM_MAP_PARAM_FIELD_FLAGS;
  mem_map_params.address = region_;
  mem_map_params.length = size_;
  // TODO: HANDLE STATUS
  ucp_mem_map(context.GetContextHandle(), &mem_map_params, &ucp_mem_);
}

MemoryRegion::~MemoryRegion() {
  // TODO: HANDLE STATUS
  ucp_mem_unmap(context_.GetContextHandle(), ucp_mem_);
  std::cout << "Destroyed memory region.\n";
}

void MemoryRegion::Pack(void **rkey_buffer_p, size_t *size_p) {
//  // TODO: HANDLE STATUS
  ucs_status_t
      status = ucp_rkey_pack(context_.GetContextHandle(),
                             ucp_mem_,
                             rkey_buffer_p,
                             size_p);
  if (status != UCS_OK) {
    throw std::runtime_error("Failed packing RKEY");
  }
}

char *MemoryRegion::GetRegion() {
  return region_;
}

long MemoryRegion::GetSize() {
  return size_;
}