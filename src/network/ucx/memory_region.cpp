
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <stdlib.h>
#include <rembrandt/network/ucx/memory_region.h>

using namespace UCP;

MemoryRegion::MemoryRegion(Context &context, StorageRegion &storage_region) : context_(context),
                                                                              storage_region_(storage_region) {
  ucp_mem_map_params_t mem_map_params;
  memset(&mem_map_params, 0, sizeof(mem_map_params));

  mem_map_params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
      UCP_MEM_MAP_PARAM_FIELD_LENGTH |
      UCP_MEM_MAP_PARAM_FIELD_FLAGS;
  mem_map_params.address = storage_region_.GetLocation();
  mem_map_params.length = storage_region_.GetSize();
  ucp_mem_map(context.GetContextHandle(), &mem_map_params, &ucp_mem_);
  void *rkey_buffer;
  size_t rkey_size;
  Pack(&rkey_buffer, &rkey_size);
  rkey_.assign((char *) rkey_buffer, rkey_size);
}

MemoryRegion::~MemoryRegion() {
  ucp_mem_unmap(context_.GetContextHandle(), ucp_mem_);
  std::cout << "Destroyed memory region.\n";
}

void MemoryRegion::Pack(void **rkey_buffer_p, size_t *size_p) {
  ucs_status_t
      status = ucp_rkey_pack(context_.GetContextHandle(),
                             ucp_mem_,
                             rkey_buffer_p,
                             size_p);
  if (status != UCS_OK) {
    throw std::runtime_error("Failed packing RKEY");
  }
}

void * MemoryRegion::GetRegion() {
  return storage_region_.GetLocation();
}

size_t MemoryRegion::GetSize() {
  return storage_region_.GetSize();
}

const std::string &MemoryRegion::GetRKey() const {
  return rkey_;
}
