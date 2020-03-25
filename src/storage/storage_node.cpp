#include <rembrandt/network/basic_message.h>
#include <rembrandt/storage/segment.h>
#include <rembrandt/storage/storage_node.h>
#include <rembrandt/storage/storage_node_config.h>

StorageNode::StorageNode(UCP::Context &context,
                         UCP::MemoryRegion &memory_region,
                         RKeyServer &r_key_server,
                         MessageGenerator &message_generator,
                         StorageNodeConfig config)
    : config_(config),
      memory_region_(memory_region),
      r_key_server_(r_key_server),
      server_(context, config.server_port),
      message_generator_(message_generator) {
  segment_ = std::make_unique<Segment>(memory_region.region_, config.segment_size);
}

void StorageNode::Run() {
  r_key_server_.Run(config_.rkey_port);
  server_.Listen(this);
}

std::unique_ptr<Message> StorageNode::HandleMessage(Message &raw_message) {
  return std::make_unique<BasicMessage>(nullptr, 0);
}
//
//std::unique_ptr<Message> StorageNode::HandleMessage(Message &raw_message) {
//  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(raw_message.GetBuffer());
//  auto union_type = base_message->content_type();
//  switch (union_type) {
//    case Rembrandt::Protocol::Message_Allocate: {
//      return HandleAllocateRequest(base_message);
//    }
////    case Rembrandt::Protocol::Message_Free: {
////      return HandleCommitRequest(base_message);
////    }
//    default: {
//      throw std::runtime_error("Message type not available!");
//    }
//  }
//}
//
//std::unique_ptr<Message> StorageNode::HandleAllocateRequest(const Rembrandt::Protocol::BaseMessage *allocate_request) {
//  auto allocate_data = static_cast<const Rembrandt::Protocol::Allocate *> (allocate_request->content());
//  if (segment_->IsFree()) {
//    segment_->Allocate(allocate_data->topic_id(), allocate_data->partition_id(), allocate_data->segment_id());
//    return message_generator_.Alloc
//  }
//}
//std::unique_ptr<Message> BrokerNode::HandleCommitRequest(const Rembrandt::Protocol::BaseMessage *commit_request) {
//  auto commit_data = static_cast<const Rembrandt::Protocol::Commit *> (commit_request->content());
//  if (segment_info_.Commit(commit_data->offset())) {
//    return message_generator_.Committed(commit_request, commit_data->offset());
//  } else {
//    return message_generator_.CommitFailed(commit_request);
//  }
//}
