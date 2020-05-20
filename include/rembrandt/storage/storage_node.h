#ifndef REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_
#define REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_

#include <rembrandt/protocol/flatbuffers/rembrandt_protocol_generated.h>
#include <rembrandt/protocol/message_generator.h>
#include "rembrandt/network/server.h"
#include "rembrandt/storage/segment.h"
#include "storage_node_config.h"
#include "r_key_server.h"

class StorageNode : public MessageHandler {
 public:
  StorageNode(UCP::Worker &data_worker,
              UCP::Worker &listening_worker,
              UCP::MemoryRegion &memory_region,
              RKeyServer &r_key_server,
              MessageGenerator &message_generator,
              StorageNodeConfig config);
  std::unique_ptr<Message> HandleMessage(const Message &raw_message) override;
  void Run();
 private:
  StorageNodeConfig config_;
  UCP::MemoryRegion &memory_region_;
  RKeyServer &r_key_server_;
  Server server_;
  std::unique_ptr<Segment> segment_;
  std::unique_ptr<Message> HandleAllocateRequest(const Rembrandt::Protocol::BaseMessage *allocate_request);
  std::unique_ptr<Message> HandleRMemInfoRequest(const Rembrandt::Protocol::BaseMessage *rmem_info_request);
};

#endif //REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_
