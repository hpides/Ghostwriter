#ifndef REMBRANDT_SRC_BROKER_BROKER_NODE_H_
#define REMBRANDT_SRC_BROKER_BROKER_NODE_H_

#include <rembrandt/protocol/flatbuffers/rembrandt_protocol_generated.h>
#include <rembrandt/protocol/message_generator.h>
#include "../network/message_handler.h"
#include "../network/server.h"
#include "./broker_node_config.h"
#include "segment_info.h"

class BrokerNode : public MessageHandler {
 public:
  BrokerNode(UCP::Context &context, MessageGenerator &message_generator, BrokerNodeConfig config);
  void Run();
  std::unique_ptr<Message> HandleMessage(Message &raw_message) override;
 private:
  BrokerNodeConfig config_;
  MessageGenerator &message_generator_;
  Server server_;
  SegmentInfo segment_info_;
  std::unique_ptr<Message> HandleCommitRequest(const Rembrandt::Protocol::BaseMessage *commit_request);
  std::unique_ptr<Message> HandleStageRequest(const Rembrandt::Protocol::BaseMessage *stage_request);
};

#endif //REMBRANDT_SRC_BROKER_BROKER_NODE_H_
