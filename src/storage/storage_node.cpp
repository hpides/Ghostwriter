#include <rembrandt/network/detached_message.h>
#include <rembrandt/storage/segment.h>
#include <rembrandt/storage/storage_node.h>
#include <rembrandt/storage/storage_node_config.h>
#include <iostream>

StorageNode::StorageNode(std::unique_ptr<Server> server,
                         std::unique_ptr<UCP::MemoryRegion> memory_region,
                         std::unique_ptr<MessageGenerator> message_generator,
                         std::unique_ptr<StorageManager> storage_manager,
                         StorageNodeConfig config)
    : MessageHandler(std::move(message_generator)),
      config_(config),
      memory_region_(std::move(memory_region)),
      server_(std::move(server)),
      storage_manager_(std::move(storage_manager)) {}

void StorageNode::Run() {
  server_->Run(this);
}

void StorageNode::Stop() {
  server_->Stop();
}

std::unique_ptr<Message> StorageNode::HandleMessage(const Message &raw_message) {
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(raw_message.GetBuffer());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_AllocateRequest: {
      return HandleAllocateRequest(*base_message);
    }
    case Rembrandt::Protocol::Message_InitializeRequest: {
      return HandleInitializeRequest(*base_message);
    }
    case Rembrandt::Protocol::Message_RMemInfoRequest: {
      return HandleRMemInfoRequest(*base_message);
    }
    default: {
      throw std::runtime_error(std::to_string(union_type));
    }
  }
}

std::unique_ptr<Message> StorageNode::HandleAllocateRequest(const Rembrandt::Protocol::BaseMessage &allocate_request) {
  auto allocate_data = static_cast<const Rembrandt::Protocol::AllocateRequest *> (allocate_request.content());
  Segment *allocated = storage_manager_->Allocate(allocate_data->topic_id(),
                                                  allocate_data->partition_id(),
                                                  allocate_data->segment_id(),
                                                  allocate_data->start_offset());
  if (allocated != nullptr) {
    return message_generator_->AllocateResponse(
        *allocated,
        storage_manager_->GetOffset(allocated->GetMemoryLocation()),
        storage_manager_->GetSegmentSize(),
        allocate_request);
  } else {
    return message_generator_->AllocateException(allocate_request);
  }
}

std::unique_ptr<Message> StorageNode::HandleRMemInfoRequest(const Rembrandt::Protocol::BaseMessage &rmem_info_request) {
  uint64_t region_ptr = (uint64_t) reinterpret_cast<uintptr_t>(memory_region_->GetRegion());
  return message_generator_->RMemInfoResponse(region_ptr, memory_region_->GetRKey(), rmem_info_request);
}
