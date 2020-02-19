#include "rembrandt/broker/broker_node.h"
#include "rembrandt/protocol/rembrandt_protocol_generated.h"

BrokerNode::BrokerNode(UCP::Context &context, BrokerNodeConfig config) : server_(context,
                                                                                 config.server_port,
                                                                                 0),
                                                                         config_(config) {
  segment_info_ = SegmentInfo(TopicPartition(1, 1), config.segment_size);
}

void BrokerNode::Run() {
  server_.Listen(this);
}

Message BrokerNode::HandleMessage(Message &raw_message) {
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(raw_message.GetBuffer());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_Stage: {
      Message stage_response = HandleStageRequest(base_message);
      break;
    }
    case Rembrandt::Protocol::Message_Commit: {
      throw std::runtime_error("Not implemented!");
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}

Message BrokerNode::HandleStageRequest(const Rembrandt::Protocol::BaseMessage *stage_request) {
  auto stage_data = static_cast<const Rembrandt::Protocol::Stage *> (stage_request->content());
  uint64_t message_size = stage_data->total_size();
  if (segment_info_.HasSpace(message_size)) {
    uint64_t offset = segment_info_.Stage(message_size);

    flatbuffers::FlatBufferBuilder builder(128);
    auto staged_response = Rembrandt::Protocol::CreateStaged(
        builder,
        offset);
    auto message = Rembrandt::Protocol::CreateBaseMessage(
        builder,
        stage_request->message_id(),
        Rembrandt::Protocol::Message_Staged,
        staged_response.Union());
    builder.FinishSizePrefixed(message);
    flatbuffers::DetachedBuffer detached_buffer = builder.Release();
    return Message(std::unique_ptr<char>((char *) detached_buffer.data()), detached_buffer.size());
  } else {
    flatbuffers::FlatBufferBuilder builder(128);
    auto stage_failed_response =
        Rembrandt::Protocol::CreateStageFailed(builder, 1, builder.CreateString("Segment is full!\n"));
    auto message = Rembrandt::Protocol::CreateBaseMessage(
        builder,
        stage_request->message_id(),
        Rembrandt::Protocol::Message_StageFailed,
        stage_failed_response.Union());
    builder.FinishSizePrefixed(message);
    flatbuffers::DetachedBuffer detached_buffer = builder.Release();
    return Message(std::unique_ptr<char>((char *) detached_buffer.data()), detached_buffer.size());
  }

}