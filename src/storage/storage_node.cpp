#include <rembrandt/network/detached_message.h>
#include <rembrandt/storage/segment.h>
#include <rembrandt/storage/storage_node.h>
#include <rembrandt/storage/storage_node_config.h>

StorageNode::StorageNode(UCP::Context &context,
                         UCP::Worker &data_worker,
                         UCP::Worker &listening_worker,
                         UCP::MemoryRegion &memory_region,
                         RKeyServer &r_key_server,
                         MessageGenerator &message_generator,
                         StorageNodeConfig config)
    : MessageHandler(message_generator),
      config_(config),
      memory_region_(memory_region),
      r_key_server_(r_key_server),
      server_(context, data_worker, listening_worker, config.server_port) {
  segment_ = std::make_unique<Segment>(memory_region.GetRegion(), memory_region.GetSize());
}

void StorageNode::Run() {
  r_key_server_.Run(config_.rkey_port);
  server_.Run(this);
}

std::unique_ptr<Message> StorageNode::HandleMessage(Message &raw_message) {
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(raw_message.GetBuffer());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_Allocate: {
      return HandleAllocateRequest(base_message);
    }
//    case Rembrandt::Protocol::Message_Free: {
//      return HandleCommitRequest(base_message);
//    }
    case Rembrandt::Protocol::Message_Initialize: {
      return HandleInitialize(base_message);
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}

std::unique_ptr<Message> StorageNode::HandleAllocateRequest(const Rembrandt::Protocol::BaseMessage *allocate_request) {
  auto allocate_data = static_cast<const Rembrandt::Protocol::Allocate *> (allocate_request->content());
  if (segment_->IsFree()) {
    segment_->Allocate(allocate_data->topic_id(), allocate_data->partition_id(), allocate_data->segment_id());
    return message_generator_.Allocated(allocate_request, *segment_);
  } else {
    return message_generator_.AllocateFailed(allocate_request);
  }
}
