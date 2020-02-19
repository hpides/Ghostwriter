#ifndef REMBRANDT_SRC_BROKER_BROKER_NODE_H_
#define REMBRANDT_SRC_BROKER_BROKER_NODE_H_

#include <rembrandt/protocol/rembrandt_protocol_generated.h>
#include "../network/message_handler.h"
#include "../network/server.h"
#include "./broker_node_config.h"
#include "segment_info.h"

class BrokerNode : public MessageHandler {
 public:
  BrokerNode(UCP::Context &context, BrokerNodeConfig config);
  void Run();
  Message HandleMessage(Message &raw_message) override;
 private:
  BrokerNodeConfig config_;
  Server server_;
  SegmentInfo segment_info_;
  Message HandleStageRequest(const Rembrandt::Protocol::BaseMessage *stage_request);
};

#endif //REMBRANDT_SRC_BROKER_BROKER_NODE_H_
