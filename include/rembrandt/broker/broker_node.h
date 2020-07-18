#ifndef REMBRANDT_SRC_BROKER_BROKER_NODE_H_
#define REMBRANDT_SRC_BROKER_BROKER_NODE_H_

#include <unordered_map>

#include <rembrandt/protocol/flatbuffers/rembrandt_protocol_generated.h>
#include <rembrandt/protocol/message_generator.h>
#include <rembrandt/network/connection_manager.h>
#include <rembrandt/storage/storage_manager.h>
#include "../network/message_handler.h"
#include "../network/server.h"
#include "./broker_node_config.h"
#include "index.h"

class BrokerNode : public MessageHandler {
 public:
  BrokerNode(std::unique_ptr<Server> server,
             ConnectionManager &connection_manager,
             std::unique_ptr<MessageGenerator> message_generator,
             RequestProcessor &request_processor,
             std::unique_ptr<UCP::Worker> client_worker,
             BrokerNodeConfig config);
  void Run();
  std::unique_ptr<Message> HandleMessage(const Message &raw_message) override;
 private:
  BrokerNodeConfig config_;
  ConnectionManager &connection_manager_;
  RequestProcessor &request_processor_;
  std::unique_ptr<UCP::Worker> client_worker_;
  std::unique_ptr<Server> server_;
  std::unordered_map<PartitionIdentifier, std::unique_ptr<Index>, PartitionIdentifierHash> segment_indices_;
  std::unique_ptr<Message> HandleCommitRequest(const Rembrandt::Protocol::BaseMessage &commit_request);
  std::unique_ptr<Message> HandleReadSegmentRequest(const Rembrandt::Protocol::BaseMessage &read_segment_request);
  std::unique_ptr<Message> HandleStageMessageRequest(const Rembrandt::Protocol::BaseMessage &stage_message_request);
  std::unique_ptr<Message> HandleStageOffsetRequest(const Rembrandt::Protocol::BaseMessage &stage_offset_request);
  std::unique_ptr<Message> HandleFetchRequest(const Rembrandt::Protocol::BaseMessage &fetch_request);
  LogicalSegment &GetWriteableSegment(uint32_t topic_id, uint32_t partition_id, uint64_t message_size);
  void AllocateSegment(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id);
  bool Commit(uint32_t topic_id, uint32_t partition_id, uint64_t offset);
  std::pair<uint64_t, uint64_t> Stage(uint32_t topic_id, uint32_t partition_id, uint64_t message_size);
  bool StageOffset(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id, uint64_t offset);
  void SendMessage(const Message &message, const UCP::Endpoint &endpoint);
  void WaitUntilReadyToReceive(const UCP::Endpoint &endpoint);
  void ReceiveAllocatedSegment(const UCP::Endpoint &endpoint,
                               uint32_t topic_id,
                               uint32_t partition_id,
                               uint32_t segment_id,
                               uint64_t start_offset);
  Index &GetIndex(uint32_t topic_id, uint32_t partition_id) const;
};

#endif //REMBRANDT_SRC_BROKER_BROKER_NODE_H_
