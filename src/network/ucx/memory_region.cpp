
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <stdlib.h>
#include <rembrandt/network/ucx/memory_region.h>

#define REGION_SIZE (16 * 1024 * 1024) // 16 MB

using namespace UCP;

MemoryRegion::MemoryRegion(Context &context) :
    context_(context) {
  region_ = malloc(REGION_SIZE);
  memset(region_, 0, REGION_SIZE);

  ucp_mem_map_params_t mem_map_params;
  memset(&mem_map_params, 0, sizeof(mem_map_params));

  mem_map_params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
      UCP_MEM_MAP_PARAM_FIELD_LENGTH |
      UCP_MEM_MAP_PARAM_FIELD_FLAGS;
  mem_map_params.address = region_;
  mem_map_params.length = REGION_SIZE;

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
