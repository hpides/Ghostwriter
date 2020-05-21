#ifndef REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_
#define REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_

#include <rembrandt/protocol/flatbuffers/rembrandt_protocol_generated.h>
#include <rembrandt/protocol/message_generator.h>
#include "rembrandt/network/server.h"
#include "rembrandt/storage/segment.h"
#include "storage_node_config.h"

class StorageNode : public MessageHandler {
 public:
  StorageNode(std::unique_ptr<Server> server,
              std::unique_ptr<UCP::MemoryRegion> memory_region,
              std::unique_ptr<MessageGenerator> message_generator,
              StorageNodeConfig config);
  std::unique_ptr<Message> HandleMessage(const Message &raw_message) override;
  void Run();
  void Stop();
 private:
  StorageNodeConfig config_;
  std::unique_ptr<UCP::MemoryRegion> memory_region_;
  std::unique_ptr<Server> server_;
  std::unique_ptr<Segment> segment_;
  std::unique_ptr<Message> HandleAllocateRequest(const Rembrandt::Protocol::BaseMessage *allocate_request);
  std::unique_ptr<Message> HandleRMemInfoRequest(const Rembrandt::Protocol::BaseMessage *rmem_info_request);
};

#endif //REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_
