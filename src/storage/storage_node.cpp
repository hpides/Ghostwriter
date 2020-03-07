#include "rembrandt/storage/segment.h"
#include "rembrandt/storage/storage_node.h"
#include "rembrandt/storage/storage_node_config.h"

StorageNode::StorageNode(UCP::Context &context,
                         UCP::MemoryRegion &memory_region,
                         RKeyServer &r_key_server,
                         StorageNodeConfig config)
    : config_(config),
      memory_region_(memory_region),
      r_key_server_(r_key_server),
      server_(context, config.server_port) {
  segment_ = std::make_unique<Segment>(memory_region.region_, config.segment_size);
}

Message StorageNode::HandleMessage(Message &raw_message) {
  return Message(nullptr, 0);
}

void StorageNode::Run() {
  r_key_server_.Run(config_.rkey_port);
  server_.Listen(this);
}
