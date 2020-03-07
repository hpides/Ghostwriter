#ifndef REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_
#define REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_

#include <rembrandt/protocol/rembrandt_protocol_generated.h>
#include "rembrandt/network/server.h"
#include "rembrandt/storage/segment.h"
#include "storage_node_config.h"
#include "r_key_server.h"

class StorageNode : public MessageHandler {
 public:
  StorageNode(UCP::Context &context,
              UCP::MemoryRegion &memory_region,
              RKeyServer &r_key_server,
              StorageNodeConfig config);
  Message HandleMessage(Message &raw_message) override;
  void Run();
 private:
  StorageNodeConfig config_;
  UCP::MemoryRegion &memory_region_;
  RKeyServer &r_key_server_;
  Server server_;
  std::shared_ptr<Segment> segment_;
};

#endif //REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_
