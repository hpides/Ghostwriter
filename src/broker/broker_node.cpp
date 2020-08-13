#include "rembrandt/broker/broker_node.h"
#include <iostream>

BrokerNode::BrokerNode(std::unique_ptr<Server> server,
                       ConnectionManager &connection_manager,
                       std::unique_ptr<MessageGenerator> message_generator,
                       RequestProcessor &request_processor,
                       std::unique_ptr<UCP::Worker> client_worker,
                       BrokerNodeConfig config)
    : MessageHandler(std::move(message_generator)),
      config_(config),
      connection_manager_(connection_manager),
      request_processor_(request_processor),
      client_worker_(std::move(client_worker)),
      server_(std::move(server)),
      partitions_() {}

void BrokerNode::AssignPartition(uint32_t topic_id, uint32_t partition_id, Partition::Mode mode) {
  PartitionIdentifier partition_identifier(topic_id, partition_id);
  if (partitions_.count(partition_identifier)) {
    throw std::runtime_error("Partition already assigned!");
  }
  partitions_[partition_identifier] = std::make_unique<Partition>(partition_identifier, mode);
  AllocateSegment(topic_id, partition_id, 1, 0);
}
void BrokerNode::Run() {
  server_->Run(this);
}

std::unique_ptr<Message> BrokerNode::HandleMessage(const Message &raw_message) {
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(raw_message.GetBuffer());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_CommitRequest: {
      return HandleCommitRequest(*base_message);
    }
    case Rembrandt::Protocol::Message_InitializeRequest: {
      return HandleInitializeRequest(*base_message);
    }
    case Rembrandt::Protocol::Message_StageRequest: {
      return HandleStageRequest(*base_message);
    }
    case Rembrandt::Protocol::Message_FetchRequest: {
      return HandleFetchRequest(*base_message);
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}

bool BrokerNode::Commit(uint32_t topic_id, uint32_t partition_id, uint64_t offset) {
  LogicalSegment &logical_segment = GetPartition(topic_id, partition_id).GetLatest();
  logical_segment.Stage(offset - logical_segment.GetWriteOffset());
  if (!logical_segment.CanCommit(offset)) return false;
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.storage_node_ip,
                                                              config_.storage_node_port,
                                                              true);
  uint64_t compare = logical_segment.GetCommitOffset() | Segment::WRITEABLE_BIT;
  uint64_t swap = offset | Segment::WRITEABLE_BIT;
  // Internally, we use the write offset to store the commit offset (since the write offset is not specified for the exclusive mode and only added for the concurrent version)
  // This enables to unify segment allocation for both modes.
  ucs_status_ptr_t status_ptr = endpoint.CompareAndSwap(compare, &swap, sizeof(swap),
                                                        endpoint.GetRemoteAddress()
                                                            + logical_segment.GetPhysicalSegment().GetLocationOfWriteOffset(),
                                                        empty_cb);
  ucs_status_t status = request_processor_.Process(status_ptr);

  if (status != UCS_OK || compare != swap) {
    throw std::runtime_error("Failed storing batch!\n");
  }
  return logical_segment.Commit(offset);
}

bool BrokerNode::ConcurrentCommit(uint32_t topic_id, uint32_t partition_id, uint64_t offset) {
  LogicalSegment *logical_segment = GetPartition(topic_id, partition_id).GetSegment(offset);
  if (!logical_segment || !logical_segment->CanCommit(offset)) {
    return false;
  }
// TODO: Implement full failure handling for paper instead of mocked protocol version for thesis.
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.storage_node_ip,
                                                              config_.storage_node_port,
                                                              true);
  uint64_t remote_location = endpoint.GetRemoteAddress()
      + logical_segment->GetPhysicalSegment().GetLocationOfData()
      + logical_segment->GetOffsetInSegment(offset - sizeof(COMMIT_FLAG));
  ucs_status_ptr_t status_ptr = endpoint.put(&COMMIT_FLAG, sizeof(COMMIT_FLAG), remote_location, empty_cb);
  ucs_status_ptr_t flush_ptr = endpoint.flush(empty_cb);
  ucs_status_t status = request_processor_.Process(status_ptr);
  ucs_status_t flush = request_processor_.Process(flush_ptr);

  if (flush != UCS_OK || status != UCS_OK) {
    throw std::runtime_error("Failed storing batch!\n");
  }
  return logical_segment->Commit(offset);
}

std::unique_ptr<Message> BrokerNode::HandleCommitRequest(const Rembrandt::Protocol::BaseMessage &commit_request) {
  auto commit_data = static_cast<const Rembrandt::Protocol::CommitRequest *> (commit_request.content());
  uint64_t offset = commit_data->logical_offset() + commit_data->message_size();
  Partition &partition = GetPartition(commit_data->topic_id(), commit_data->partition_id());
  bool committed = false;
  switch (partition.GetMode()) {
    case Partition::Mode::EXCLUSIVE:committed = Commit(commit_data->topic_id(), commit_data->partition_id(), offset);
      break;
    case Partition::Mode::CONCURRENT:
      committed = ConcurrentCommit(commit_data->topic_id(), commit_data->partition_id(), offset);
      break;
  }
  if (committed) {
    return message_generator_->CommitResponse(offset, commit_request);
  } else {
    return message_generator_->CommitException(commit_request);
  }
}

RemoteBatch BrokerNode::Stage(uint32_t topic_id,
                              uint32_t partition_id,
                              uint64_t message_size,
                              uint64_t max_batch) {
  LogicalSegment &logical_segment = GetWriteableSegment(topic_id, partition_id, message_size);
  uint64_t logical_offset = logical_segment.GetCommitOffset();
  uint64_t physical_offset =
      logical_segment.GetOffsetInSegment(logical_offset) + logical_segment.GetPhysicalSegment().GetLocationOfData();
  uint64_t batch = logical_segment.GetSpace() / message_size;
  return RemoteBatch(logical_offset, physical_offset, message_size, std::min(batch, max_batch));
}

RemoteBatch BrokerNode::ConcurrentStage(uint32_t topic_id,
                                        uint32_t partition_id,
                                        uint64_t message_size,
                                        uint64_t max_batch_size) {
  message_size = GetConcurrentMessageSize(message_size);
  LogicalSegment &logical_segment = GetWriteableSegment(topic_id, partition_id, message_size);
  uint64_t batch_size = std::min(max_batch_size, logical_segment.GetSpace() / message_size);
  uint64_t logical_offset = logical_segment.GetWriteOffset();
  uint64_t compare = logical_offset | Segment::WRITEABLE_BIT;
  uint64_t staged_offset = logical_segment.Stage(message_size * batch_size);
  uint64_t swap = staged_offset | Segment::WRITEABLE_BIT;
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.storage_node_ip, config_.storage_node_port, true);
  uint64_t storage_addr = endpoint.GetRemoteAddress()
      + logical_segment.GetPhysicalSegment().GetLocationOfWriteOffset();
  ucs_status_ptr_t
      status_ptr = endpoint.CompareAndSwap(compare, &swap, sizeof(swap), storage_addr, empty_cb);
  ucs_status_t status = request_processor_.Process(status_ptr);
  if (status != UCS_OK || compare != swap) {
    throw std::runtime_error("Persisting write offset failed");
  }
  uint64_t physical_offset =
      logical_segment.GetOffsetInSegment(logical_offset) + logical_segment.GetPhysicalSegment().GetLocationOfData();
  return RemoteBatch(logical_offset, physical_offset, message_size, batch_size);
}

void BrokerNode::CloseSegment(LogicalSegment &logical_segment) {
  uint64_t compare = logical_segment.GetWriteOffset() | Segment::WRITEABLE_BIT;
  uint64_t staged_offset = logical_segment.GetWriteOffset() & ~Segment::WRITEABLE_BIT;
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.storage_node_ip, config_.storage_node_port, true);
  uint64_t storage_addr = endpoint.GetRemoteAddress()
      + logical_segment.GetPhysicalSegment().GetLocationOfWriteOffset();
  ucs_status_ptr_t
      status_ptr = endpoint.CompareAndSwap(compare, &staged_offset, sizeof(staged_offset), storage_addr, empty_cb);
  ucs_status_t status = request_processor_.Process(status_ptr);
  if (status != UCS_OK || compare != staged_offset) {
    throw std::runtime_error("Failed closing segment");
  }
}

uint64_t BrokerNode::GetConcurrentMessageSize(uint64_t message_size) {
  return message_size + sizeof(TIMEOUT_FLAG);
}

std::unique_ptr<Message> BrokerNode::HandleStageRequest(const Rembrandt::Protocol::BaseMessage &stage_request) {
  auto stage_data = static_cast<const Rembrandt::Protocol::StageRequest *> (stage_request.content());
  Partition &partition = GetPartition(stage_data->topic_id(), stage_data->partition_id());
  RemoteBatch remote_batch = RemoteBatch();
  switch (partition.GetMode()) {
    case Partition::Mode::EXCLUSIVE:
      remote_batch = Stage(stage_data->topic_id(),
                           stage_data->partition_id(),
                           stage_data->message_size(),
                           stage_data->max_batch());
      break;
    case Partition::Mode::CONCURRENT:
      remote_batch = ConcurrentStage(stage_data->topic_id(),
                                     stage_data->partition_id(),
                                     stage_data->message_size(),
                                     stage_data->max_batch());

      break;
  }
  return message_generator_->StageResponse(remote_batch.logical_offset_,
                                           remote_batch.remote_location_,
                                           remote_batch.effective_message_size_,
                                           remote_batch.batch_size_,
                                           stage_request);
}

std::unique_ptr<Message> BrokerNode::HandleFetchRequest(const Rembrandt::Protocol::BaseMessage &fetch_request) {
  auto fetch_data = static_cast<const Rembrandt::Protocol::FetchRequest *> (fetch_request.content());
  Partition &index = GetPartition(fetch_data->topic_id(), fetch_data->partition_id());
  LogicalSegment *logical_segment = index.GetSegment(fetch_data->logical_offset());
  if (logical_segment == nullptr || logical_segment->GetCommitOffset() <= fetch_data->logical_offset()) {
    return message_generator_->FetchException(fetch_request);
  }
  PhysicalSegment &physical_segment = logical_segment->GetPhysicalSegment();
  uint64_t remote_location =
      physical_segment.GetLocationOfData() + logical_segment->GetOffsetInSegment(fetch_data->logical_offset());
  return message_generator_->FetchResponse(remote_location,
                                           logical_segment->GetCommitOffset(),
                                           fetch_request);
}

void BrokerNode::AllocateSegment(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id, uint64_t start_offset) {
  std::unique_ptr<Message> allocate_message = message_generator_->AllocateRequest(topic_id,
                                                                                  partition_id,
                                                                                  segment_id,
                                                                                  start_offset);
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.storage_node_ip,
                                                              config_.storage_node_port);
  SendMessage(*allocate_message, endpoint);
  WaitUntilReadyToReceive(endpoint);
  ReceiveAllocatedSegment(endpoint, topic_id, partition_id, segment_id, start_offset);
}

LogicalSegment &BrokerNode::GetWriteableSegment(uint32_t topic_id, uint32_t partition_id, uint64_t message_size) {
  Partition &index = GetPartition(topic_id, partition_id);
  if (index.IsEmpty()) {
    AllocateSegment(topic_id, partition_id, 1, 0);
  } else {
    LogicalSegment &latest = index.GetLatest();
    if (!latest.IsWriteable()) {
      AllocateSegment(topic_id, partition_id, latest.GetSegmentId() + 1, latest.GetWriteOffset());
    } else if (!latest.HasSpace(message_size)) {
      CloseSegment(latest);
      AllocateSegment(topic_id, partition_id, latest.GetSegmentId() + 1, latest.GetWriteOffset());
    }
  }
  return index.GetLatest();
}
void BrokerNode::ReceiveAllocatedSegment(const UCP::Endpoint &endpoint,
                                         uint32_t topic_id,
                                         uint32_t partition_id,
                                         uint32_t segment_id,
                                         uint64_t start_offset) {
  std::unique_ptr<Message> response = server_->ReceiveMessage(endpoint);
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(response->GetBuffer());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_AllocateResponse: {
      auto allocate_response = static_cast<const Rembrandt::Protocol::AllocateResponse *> (base_message->content());
      std::unique_ptr<PhysicalSegment>
          physical_segment = std::make_unique<PhysicalSegment>(allocate_response->offset());
      std::unique_ptr<LogicalSegment>
          logical_segment = std::make_unique<LogicalSegment>(SegmentIdentifier(topic_id, partition_id, segment_id),
                                                             std::move(physical_segment),
                                                             start_offset,
                                                             allocate_response->size());
      GetPartition(topic_id, partition_id).Append(std::move(logical_segment));
      break;
    }
    case Rembrandt::Protocol::Message_AllocateException: {
      throw std::runtime_error("Handling AllocateException not implemented!");
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}

void BrokerNode::SendMessage(const Message &message, const UCP::Endpoint &endpoint) {
  ucs_status_ptr_t ucs_status_ptr = endpoint.send(message.GetBuffer(), message.GetSize());
  ucs_status_t status = request_processor_.Process(ucs_status_ptr);
  if (status != UCS_OK) {
    throw std::runtime_error("Failed sending request!\n");
  }
}

void BrokerNode::WaitUntilReadyToReceive(const UCP::Endpoint &endpoint) {
  ucp_stream_poll_ep_t *stream_poll_eps = (ucp_stream_poll_ep_t *) malloc(sizeof(ucp_stream_poll_ep_t) * 5);
  while (true) {
    ssize_t num_eps = ucp_stream_worker_poll(client_worker_->GetWorkerHandle(), stream_poll_eps, 5, 0);
    if (num_eps > 0) {
      if (stream_poll_eps->ep == endpoint.GetHandle()) {
        break;
      }
    } else if (num_eps < 0) {
      throw std::runtime_error("Error!");
    } else {
      client_worker_->Progress();
    }
  }
  free(stream_poll_eps);
}

Partition &BrokerNode::GetPartition(uint32_t topic_id, uint32_t partition_id) const {
  return *(partitions_.at(PartitionIdentifier(topic_id, partition_id)).get());
}
