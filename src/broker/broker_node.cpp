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
      segment_indices_() {
  segment_indices_[PartitionIdentifier(1, 1)] = std::make_unique<Index>(PartitionIdentifier(1, 1));
  AllocateSegment(1, 1, 1);
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
    case Rembrandt::Protocol::Message_ReadSegmentRequest: {
      return HandleReadSegmentRequest(*base_message);
    }
    case Rembrandt::Protocol::Message_StageMessageRequest: {
      return HandleStageMessageRequest(*base_message);
    }
    case Rembrandt::Protocol::Message_StageOffsetRequest: {
      return HandleStageOffsetRequest(*base_message);
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
  LogicalSegment &logical_segment = GetIndex(topic_id, partition_id).GetLatest();
  logical_segment.Stage(offset - logical_segment.GetCommitOffset());
  if (!logical_segment.CanCommit(offset)) return false;
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.storage_node_ip,
                                                              config_.storage_node_port,
                                                              true);
  uint64_t swap = offset | Segment::COMMITTABLE_BIT;
  ucs_status_ptr_t status_ptr = endpoint.put(&swap, sizeof(swap),
                                             endpoint.GetRemoteAddress()
                                                 + logical_segment.GetPhysicalSegment().GetLocationOfCommitOffset(),
                                             empty_cb);
  ucs_status_t status = request_processor_.Process(status_ptr);
    if (status != UCS_OK) {
    return false;
  }
  return logical_segment.Commit(offset);
}

std::unique_ptr<Message> BrokerNode::HandleCommitRequest(const Rembrandt::Protocol::BaseMessage &commit_request) {
  auto commit_data = static_cast<const Rembrandt::Protocol::CommitRequest *> (commit_request.content());
  uint64_t offset = commit_data->logical_offset() + commit_data->message_size();
  if (Commit(commit_data->topic_id(), commit_data->partition_id(), offset)) {
    return message_generator_->CommitResponse(offset, commit_request);
  } else {
    return message_generator_->CommitException(commit_request);
  }
}

std::pair<uint64_t, uint64_t> BrokerNode::Stage(uint32_t topic_id, uint32_t partition_id, uint64_t message_size) {
  // TODO: ADJUST
  LogicalSegment &logical_segment = GetWriteableSegment(topic_id, partition_id, message_size);
  uint64_t logical_offset = logical_segment.GetCommitOffset();
//  uint64_t staged_value = logical_segment.Stage(message_size) | Segment::WRITEABLE_BIT;
//  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.storage_node_ip, config_.storage_node_port, true);
//  uint64_t storage_addr = endpoint.GetRemoteAddress()
//      + logical_segment.GetPhysicalSegment().GetLocationOfWriteOffset();
//  ucs_status_ptr_t status_ptr = endpoint.put(&staged_value, sizeof(staged_value), storage_addr, empty_cb);
//  ucs_status_t status = request_processor_.Process(status_ptr);
//  if (status != UCS_OK) {
//     TODO: Handle failure case
//    return std::pair<uint32_t, uint64_t>(0, 0);
//  }
  uint64_t physical_offset = logical_segment.GetOffsetInSegment(logical_offset) + logical_segment.GetPhysicalSegment().GetLocationOfData();
  return std::pair<uint64_t, uint64_t>(logical_offset, physical_offset);
}

bool BrokerNode::StageOffset(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id, uint64_t offset) {
  uint64_t commit_offset = GetIndex(topic_id, partition_id).GetCommitOffset();
  LogicalSegment &logical_segment = GetWriteableSegment(topic_id, segment_id, offset - commit_offset);
  return true;
}

std::unique_ptr<Message> BrokerNode::HandleReadSegmentRequest(const Rembrandt::Protocol::BaseMessage &read_segment_request) {
  auto read_segment_data = static_cast<const Rembrandt::Protocol::ReadSegmentRequest *>(read_segment_request.content());
  Index &index = GetIndex(read_segment_data->topic_id(), read_segment_data->partition_id());
  LogicalSegment *logical_segment = index.GetSegmentById(read_segment_data->segment_id());
  if (logical_segment == nullptr) {
    return message_generator_->ReadSegmentException(read_segment_request);
  }
  PhysicalSegment &physical_segment = logical_segment->GetPhysicalSegment();
  return message_generator_->ReadSegmentResponse(logical_segment->GetTopicId(),
                                                 logical_segment->GetPartitionId(),
                                                 logical_segment->GetSegmentId(),
                                                 physical_segment.GetLocationOfData(),
                                                 logical_segment->GetCommitOffset(),
                                                 logical_segment->IsCommittable(),
                                                 read_segment_request);
}

std::unique_ptr<Message> BrokerNode::HandleStageMessageRequest(const Rembrandt::Protocol::BaseMessage &stage_message_request) {
  auto stage_data = static_cast<const Rembrandt::Protocol::StageMessageRequest *> (stage_message_request.content());
  auto[segment_id, offset] = Stage(stage_data->topic_id(), stage_data->partition_id(), stage_data->message_size());
  return message_generator_->StageMessageResponse(segment_id, offset, stage_message_request);
}

std::unique_ptr<Message> BrokerNode::HandleStageOffsetRequest(const Rembrandt::Protocol::BaseMessage &stage_offset_request) {
  auto stage_data = static_cast<const Rembrandt::Protocol::StageOffsetRequest *>(stage_offset_request.content());
  bool staged =
      StageOffset(stage_data->topic_id(), stage_data->partition_id(), stage_data->segment_id(), stage_data->offset());
  if (staged) {
    return message_generator_->StageOffsetResponse(stage_offset_request);
  } else {
    return message_generator_->StageOffsetException(stage_offset_request);
  }
}

std::unique_ptr<Message> BrokerNode::HandleFetchRequest(const Rembrandt::Protocol::BaseMessage &fetch_request) {
  // TODO: FIX/REMOVE
  auto fetch_data = static_cast<const Rembrandt::Protocol::FetchRequest *> (fetch_request.content());
  Index &index = GetIndex(fetch_data->topic_id(), fetch_data->partition_id());
  LogicalSegment *logical_segment = index.GetSegmentById(fetch_data->segment_id());
  if (logical_segment == nullptr) {
    return message_generator_->FetchException(fetch_request);
  }
  PhysicalSegment &physical_segment = logical_segment->GetPhysicalSegment();
  return message_generator_->FetchResponse(
      physical_segment.GetLocationOfData(),
      physical_segment.GetLocationOfCommitOffset(),
      logical_segment->IsCommittable(),
      fetch_request);
}

void BrokerNode::AllocateSegment(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id) {
  std::unique_ptr<Message> allocate_message = message_generator_->AllocateRequest(topic_id, partition_id, segment_id);
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.storage_node_ip,
                                                              config_.storage_node_port);
  SendMessage(*allocate_message, endpoint);
  WaitUntilReadyToReceive(endpoint);
  ReceiveAllocatedSegment(endpoint, topic_id, partition_id, segment_id, 0);
}

LogicalSegment &BrokerNode::GetWriteableSegment(uint32_t topic_id, uint32_t partition_id, uint64_t message_size) {
  Index &index = GetIndex(topic_id, partition_id);
  if (index.IsEmpty()) {
    AllocateSegment(topic_id, partition_id, 1);
  } else {
    LogicalSegment &latest = index.GetLatest();
    if (!latest.IsWriteable()) {
      AllocateSegment(topic_id, partition_id, latest.GetSegmentId() + 1);
    } else if (!latest.HasSpace(message_size)) {
      // TODO Close for writes
//      CloseForWrites(last);
      AllocateSegment(topic_id, partition_id, latest.GetSegmentId() + 1);
    }
  }
  // TODO: Check message size <= segment_size
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
      GetIndex(topic_id, partition_id).Append(std::move(logical_segment));
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
  // TODO: Adjust to handling different response types
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

Index &BrokerNode::GetIndex(uint32_t topic_id, uint32_t partition_id) const {
  return *(segment_indices_.at(PartitionIdentifier(topic_id, partition_id)).get());
}