#include "rembrandt/storage/segment.h"
#include "rembrandt/storage/storage_node.h"

#include <sys/mman.h>

StorageNode::StorageNode(UCP::Context &context,
                         uint64_t region_size,
                         uint64_t segment_size,
                         uint32_t server_port,
                         uint32_t rkey_port) : server_(context,
                                                       server_port,
                                                       rkey_port) {
  void *memory_region = malloc(region_size);
  // TODO: Use multiple segments
  segment_ = std::make_unique<Segment>(memory_region, segment_size);
}

void StorageNode::Run() {
  server_.Listen();
}

int main(int argc, char *argv[]) {
  UCP::Context context = UCP::Context(true);
  StorageNode storage_node = StorageNode(context,
                                         1024,
                                         1024,
                                         13350,
                                         13351);
  storage_node.Run();
}
