#ifndef REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_
#define REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_

#include <rembrandt/protocol/flatbuffers/rembrandt_protocol_generated.h>
#include <rembrandt/protocol/message_generator.h>
#include "rembrandt/network/server.h"
#include "rembrandt/storage/segment.h"
#include "rembrandt/storage/storage_manager.h"
#include "rembrandt/storage/storage_region.h"
#include "storage_node_config.h"
#include <array>
#include <queue>

class StorageNode : public MessageHandler {
 public:
   static StorageNode Create(StorageNodeConfig config, UCP::Context &context);
  std::unique_ptr<Message> HandleMessage(const Message &raw_message) override;
  void Run();
  void Stop();
 private:
  StorageNode(std::unique_ptr<Server> server,
              std::unique_ptr<UCP::MemoryRegion> memory_region,
              std::unique_ptr<MessageGenerator> message_generator,
              std::unique_ptr<StorageManager> storage_manager,
              StorageNodeConfig config);
  StorageNodeConfig config_;
  std::unique_ptr<UCP::MemoryRegion> memory_region_;
  std::unique_ptr<Server> server_;
  std::unique_ptr<StorageManager> storage_manager_;
  std::unique_ptr<Message> HandleAllocateRequest(const Rembrandt::Protocol::BaseMessage &allocate_request);
  std::unique_ptr<Message> HandleRMemInfoRequest(const Rembrandt::Protocol::BaseMessage &rmem_info_request);
};

#endif //REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_
