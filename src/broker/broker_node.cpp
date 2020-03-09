#include "rembrandt/broker/broker_node.h"

BrokerNode::BrokerNode(UCP::Context &context, MessageGenerator &message_generator, BrokerNodeConfig config)
    : config_(config),
      message_generator_(message_generator),
      server_(context, config.server_port),
      segment_info_(TopicPartition(1, 1),
                    config.segment_size) {}

void BrokerNode::Run() {
  server_.Listen(this);
}

std::unique_ptr<Message> BrokerNode::HandleMessage(Message &raw_message) {
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(raw_message.GetBuffer());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_Stage: {
      return HandleStageRequest(base_message);
    }
    case Rembrandt::Protocol::Message_Commit: {
      return HandleCommitRequest(base_message);
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}

std::unique_ptr<Message> BrokerNode::HandleCommitRequest(const Rembrandt::Protocol::BaseMessage *commit_request) {
  auto commit_data = static_cast<const Rembrandt::Protocol::Commit *> (commit_request->content());
  if (segment_info_.Commit(commit_data->offset())) {
    return message_generator_.Committed(commit_request, commit_data->offset());
  } else {
    return message_generator_.CommitFailed(commit_request);
  }
}
std::unique_ptr<Message> BrokerNode::HandleStageRequest(const Rembrandt::Protocol::BaseMessage *stage_request) {
  auto stage_data = static_cast<const Rembrandt::Protocol::Stage *> (stage_request->content());
  uint64_t message_size = stage_data->total_size();
  if (segment_info_.HasSpace(message_size)) {
    uint64_t offset = segment_info_.Stage(message_size);
    return message_generator_.Staged(stage_request, offset);
  } else {
    return message_generator_.StageFailed(stage_request);
  }
}
