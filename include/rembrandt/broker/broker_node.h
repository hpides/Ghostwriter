#ifndef REMBRANDT_SRC_BROKER_BROKER_NODE_H_
#define REMBRANDT_SRC_BROKER_BROKER_NODE_H_

#include <rembrandt/protocol/flatbuffers/rembrandt_protocol_generated.h>
#include <rembrandt/protocol/message_generator.h>
#include <rembrandt/network/connection_manager.h>
#include "../network/message_handler.h"
#include "../network/server.h"
#include "./broker_node_config.h"
#include "segment_info.h"

class BrokerNode : public MessageHandler {
 public:
  BrokerNode(UCP::Context &context,
             ConnectionManager &connection_manager,
             MessageGenerator &message_generator,
             RequestProcessor &request_processor,
             UCP::Worker &worker,
             BrokerNodeConfig config);
  void Run();
  std::unique_ptr<Message> HandleMessage(Message &raw_message) override;
 private:
  BrokerNodeConfig config_;
  ConnectionManager &connection_manager_;
  MessageGenerator &message_generator_;
  RequestProcessor &request_processor_;
  UCP::Worker &worker_;
  Server server_;
  std::unique_ptr<SegmentInfo> segment_info_;
  std::unique_ptr<Message> HandleCommitRequest(const Rembrandt::Protocol::BaseMessage *commit_request);
  std::unique_ptr<Message> HandleStageRequest(const Rembrandt::Protocol::BaseMessage *stage_request);
  SegmentInfo &GetSegmentInfo(const TopicPartition &topic_partition);
  void AllocateSegment(const TopicPartition &topic_partition);
  void SendMessage(Message &message, UCP::Endpoint &endpoint);
  void WaitUntilReadyToReceive(UCP::Endpoint &endpoint);
  void ReceiveAllocatedSegment(UCP::Endpoint &endpoint, const TopicPartition &topic_partition);
  uint64_t message_counter_ = 0;
};

#endif //REMBRANDT_SRC_BROKER_BROKER_NODE_H_
