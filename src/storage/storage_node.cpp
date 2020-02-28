#include "rembrandt/storage/segment.h"
#include "rembrandt/storage/storage_node.h"
#include "rembrandt/storage/storage_node_config.h"

StorageNode::StorageNode(UCP::Context &context, StorageNodeConfig config)
    : config_(config), server_(context, config.server_port, config.rkey_port) {
  void *memory_region = malloc(config.region_size);
  segment_ = std::make_unique<Segment>(memory_region, config.segment_size);
}

StorageNode::StorageNode(UCP::Context &context,
                         uint64_t region_size,
                         uint64_t segment_size,
                         uint32_t server_port,
                         uint32_t rkey_port) : server_(context, server_port, rkey_port) {
  void *memory_region = malloc(region_size);
  // TODO: Use multiple segments
  segment_ = std::make_unique<Segment>(memory_region, segment_size);
}

Message StorageNode::HandleMessage(Message &raw_message) {
  return Message(nullptr, 0);
}

void StorageNode::Run() {
  server_.Listen(this);
}
