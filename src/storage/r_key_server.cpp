#include "rembrandt/storage/r_key_server.h"

RKeyServer::RKeyServer(UCP::MemoryRegion &memory_region) {
  void *rkey_buffer;
  size_t rkey_size;
  memory_region.Pack(&rkey_buffer, &rkey_size);
  char *data = (char *) malloc(sizeof(uint64_t) + rkey_size);
  uint64_t region_ptr = (uint64_t) reinterpret_cast<uintptr_t>(memory_region.region_);
  memcpy(data, &region_ptr, sizeof(uint64_t));
  memcpy(data + sizeof(uint64_t), rkey_buffer, rkey_size);
  ucp_rkey_buffer_release(rkey_buffer);
  std::shared_ptr<void> rkey_shared_ptr = std::shared_ptr<void>(data);
  SetPayload(rkey_shared_ptr, sizeof(uint64_t) + rkey_size);
}