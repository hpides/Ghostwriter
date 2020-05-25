#ifndef REMBRANDT_SRC_BROKER_BROKER_NODE_H_
#define REMBRANDT_SRC_BROKER_BROKER_NODE_H_

#include <rembrandt/protocol/flatbuffers/rembrandt_protocol_generated.h>
#include <rembrandt/protocol/message_generator.h>
#include <rembrandt/network/connection_manager.h>
#include <rembrandt/storage/storage_manager.h>
#include "../network/message_handler.h"
#include "../network/server.h"
#include "./broker_node_config.h"
#include "segment_info.h"

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
  std::vector<std::unique_ptr<SegmentInfo>> segment_info_;
  std::unique_ptr<Message> HandleCommitRequest(const Rembrandt::Protocol::BaseMessage *commit_request);
  std::unique_ptr<Message> HandleStageRequest(const Rembrandt::Protocol::BaseMessage *stage_request);
  std::unique_ptr<Message> HandleFetchCommittedOffsetRequest(const Rembrandt::Protocol::BaseMessage *committed_offset_request);
  std::unique_ptr<Message> HandleFetchInitialRequest(const Rembrandt::Protocol::BaseMessage *fetch_initial_request);
  SegmentInfo *GetSegmentInfo(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id);
  SegmentInfo *GetLatestSegmentInfo(uint32_t topic_id, uint32_t partition_id);
  SegmentInfo &GetWriteableSegment(uint32_t topic_id, uint32_t partition_id, uint64_t message_size);
  void AllocateSegment(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id);
  bool Commit(uint32_t topic_id, uint32_t partition_id, uint64_t offset);
  uint64_t Stage(uint32_t topic_id, uint32_t partition_id, uint64_t message_size);
  void SendMessage(const Message &message, const UCP::Endpoint &endpoint);
  void WaitUntilReadyToReceive(const UCP::Endpoint &endpoint);
  void ReceiveAllocatedSegment(const UCP::Endpoint &endpoint,
                               uint32_t topic_id,
                               uint32_t partition_id,
                               uint32_t segment_id);
};

#endif //REMBRANDT_SRC_BROKER_BROKER_NODE_H_
