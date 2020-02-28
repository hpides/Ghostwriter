#ifndef REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_
#define REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_

#include <rembrandt/protocol/rembrandt_protocol_generated.h>
#include "rembrandt/network/server.h"
#include "rembrandt/storage/segment.h"
#include "storage_node_config.h"

class StorageNode : public MessageHandler {
 public:
  StorageNode(UCP::Context &context, StorageNodeConfig config);
  StorageNode(UCP::Context &context,
              uint64_t region_size,
              uint64_t segment_size,
              uint32_t server_port,
              uint32_t rkey_port);
  Message HandleMessage(Message &raw_message) override;
  void Run();
 private:
  StorageNodeConfig config_;
  Server server_;
  std::shared_ptr<Segment> segment_;
};

#endif //REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_
