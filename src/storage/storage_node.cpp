#include <iostream>
#include <memory>
#include <rembrandt/network/detached_message.h>
#include <rembrandt/storage/persistent_storage_region.h>
#include <rembrandt/storage/volatile_storage_region.h>
#include <rembrandt/storage/segment.h>
#include <rembrandt/storage/storage_node.h>
#include <rembrandt/storage/storage_node_config.h>

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

void StorageNode::Run() { server_->Run(this); }

void StorageNode::Stop() { server_->Stop(); }

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
    return message_generator_p_->AllocateResponse(
        *allocated,
        storage_manager_->GetOffset(allocated->GetMemoryLocation()),
        storage_manager_->GetSegmentSize(),
        allocate_request);
  } else {
    return message_generator_p_->AllocateException(allocate_request);
  }
}

std::unique_ptr<Message> StorageNode::HandleRMemInfoRequest(const Rembrandt::Protocol::BaseMessage &rmem_info_request) {
  uint64_t region_ptr = (uint64_t) reinterpret_cast<uintptr_t>(memory_region_->GetRegion());
  return message_generator_p_->RMemInfoResponse(region_ptr, memory_region_->GetRKey(), rmem_info_request);
}

StorageNode StorageNode::Create(StorageNodeConfig config, UCP::Context &context) {
  std::unique_ptr<UCP::Worker> data_worker_p = context.CreateWorker();
  std::unique_ptr<UCP::Worker> listening_worker_p = context.CreateWorker();
  std::unique_ptr<Server>
      server_p = std::make_unique<Server>(std::move(data_worker_p), std::move(listening_worker_p), config.server_port);

  std::unique_ptr<MessageGenerator> message_generator_p = std::make_unique<MessageGenerator>();

  std::unique_ptr<StorageRegion> storage_region_p;

  switch(config.type) {
    case StorageNodeConfig::Type::PERSISTENT:
      storage_region_p = std::make_unique<PersistentStorageRegion>(config.region_size, alignof(SegmentHeader));
      break;
    case StorageNodeConfig::Type::VOLATILE:
      storage_region_p = std::make_unique<VolatileStorageRegion>(config.region_size, alignof(SegmentHeader));
      break;
    default:
      throw std::runtime_error("Storage node type not available: ");
  }
  std::unique_ptr<UCP::MemoryRegion> memory_region_p = context.RegisterStorageRegion(*storage_region_p);
  // storage manager
  std::unique_ptr<StorageManager>
      storage_manager_p = std::make_unique<StorageManager>(std::move(storage_region_p), config);
  return StorageNode(std::move(server_p),
                     std::move(memory_region_p),
                     std::move(message_generator_p),
                     std::move(storage_manager_p), config);
}