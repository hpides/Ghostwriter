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
      server_(std::move(server)) {}

void BrokerNode::Run() {
  server_->Run(this);
}

std::unique_ptr<Message> BrokerNode::HandleMessage(const Message &raw_message) {
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(raw_message.GetBuffer());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_Commit: {
      return HandleCommitRequest(base_message);
    }
    case Rembrandt::Protocol::Message_Initialize: {
      return HandleInitialize(base_message);
    }
    case Rembrandt::Protocol::Message_Stage: {
      return HandleStageRequest(base_message);
    }
    case Rembrandt::Protocol::Message_FetchInitial: {
      return HandleFetchInitialRequest(base_message);
    }
    case Rembrandt::Protocol::Message_FetchCommittedOffset: {
      return HandleFetchCommittedOffsetRequest(base_message);
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}

bool BrokerNode::Commit(uint32_t topic_id, uint32_t partition_id, uint64_t offset) {
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.storage_node_ip,
                                                              config_.storage_node_port,
                                                              true);
  uint64_t swap = offset | Segment::COMMITTABLE_BIT;
  SegmentInfo *segment_info = GetLatestSegmentInfo(topic_id, partition_id);
  assert(segment_info != nullptr);
  uint64_t compare = segment_info->GetCommitOffset() | Segment::COMMITTABLE_BIT;
  ucs_status_ptr_t status_ptr = endpoint.CompareAndSwap(compare,
                                                        &swap,
                                                        sizeof(swap),
                                                        endpoint.GetRemoteAddress()
                                                            + segment_info->GetOffsetOfCommitOffset(),
                                                        empty_cb);
  ucs_status_t status = request_processor_.Process(status_ptr);
  if (status != UCS_OK) {
    return false;
  }
  return segment_info->Commit(offset);
}

std::unique_ptr<Message> BrokerNode::HandleCommitRequest(const Rembrandt::Protocol::BaseMessage *commit_request) {
  auto commit_data = static_cast<const Rembrandt::Protocol::Commit *> (commit_request->content());
  SegmentInfo *segment_info = GetLatestSegmentInfo(commit_data->topic_id(), commit_data->partition_id());
  assert(segment_info != nullptr);
  // TODO: Abstract offsets away and use message ids
  uint64_t offset = commit_data->offset() - segment_info->GetOffset();
  if (segment_info->CanCommit(offset)
      && Commit(commit_data->topic_id(), commit_data->partition_id(), offset)) {
    return message_generator_->Committed(commit_request, commit_data->offset());
  } else {
    return message_generator_->CommitFailed(commit_request);
  }
}

uint64_t BrokerNode::Stage(uint32_t topic_id, uint32_t partition_id, uint64_t message_size) {
  // TODO: ADJUST
  SegmentInfo &segment_info = GetWriteableSegment(topic_id, partition_id, message_size);
  uint64_t old_offset = segment_info.GetWriteOffset();
  uint64_t compare = segment_info.GetWriteOffset() | Segment::WRITEABLE_BIT;
  uint64_t to_stage = segment_info.Stage(message_size) | Segment::WRITEABLE_BIT;
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.storage_node_ip, config_.storage_node_port, true);
  ucs_status_ptr_t status_ptr = endpoint.CompareAndSwap(compare,
                                                        &to_stage,
                                                        sizeof(to_stage),
                                                        endpoint.GetRemoteAddress()
                                                            + segment_info.GetOffsetOfWriteOffset(),
                                                        empty_cb);
  ucs_status_t status = request_processor_.Process(status_ptr);
  if (status != UCS_OK) {
    return false;
  }
  assert(compare == to_stage);
  return old_offset + segment_info.GetOffset();
}

std::unique_ptr<Message> BrokerNode::HandleStageRequest(const Rembrandt::Protocol::BaseMessage *stage_request) {
  auto stage_data = static_cast<const Rembrandt::Protocol::Stage *> (stage_request->content());
  uint64_t message_size = stage_data->total_size();
  uint64_t offset = Stage(1, 1, message_size);
  // TODO: Adjust overwriting logic in Stage()
  return message_generator_->Staged(stage_request, offset);
}

std::unique_ptr<Message> BrokerNode::HandleFetchInitialRequest(const Rembrandt::Protocol::BaseMessage *fetch_initial_request) {
  auto fetch_initial_data = static_cast<const Rembrandt::Protocol::FetchInitial *> (fetch_initial_request->content());
  SegmentInfo *segment_info = GetSegmentInfo(1, 1, 1);
  if (segment_info == nullptr) {
    return message_generator_->FetchFailed(fetch_initial_request);
  }
  return message_generator_->FetchedInitial(fetch_initial_request,
                                            segment_info->GetDataOffset(),
                                            segment_info->GetCommitOffset());
}

std::unique_ptr<Message> BrokerNode::HandleFetchCommittedOffsetRequest(const Rembrandt::Protocol::BaseMessage *committed_offset_request) {
  auto committed_offset_data =
      static_cast<const Rembrandt::Protocol::FetchCommittedOffset *> (committed_offset_request->content());
  // TODO: Adjust segment id
  SegmentInfo
      *segment_info = GetSegmentInfo(committed_offset_data->topic_id(), committed_offset_data->partition_id(), 1);
  return message_generator_->FetchedCommittedOffset(committed_offset_request, segment_info->GetCommitOffset());
}

SegmentInfo *BrokerNode::GetLatestSegmentInfo(uint32_t topic_id, uint32_t partition_id) {
  return segment_info_.back().get();
}

SegmentInfo *BrokerNode::GetSegmentInfo(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id) {
  if (segment_info_.size() < segment_id) {
    return nullptr;
  }
  return segment_info_[segment_id].get();
}

void BrokerNode::AllocateSegment(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id) {
  std::unique_ptr<Message> allocate_message = message_generator_->Allocate(topic_id, partition_id, segment_id);
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.storage_node_ip,
                                                              config_.storage_node_port);
  SendMessage(*allocate_message, endpoint);
  WaitUntilReadyToReceive(endpoint);
  ReceiveAllocatedSegment(endpoint, topic_id, partition_id, segment_id);
}

SegmentInfo &BrokerNode::GetWriteableSegment(uint32_t topic_id, uint32_t partition_id, uint64_t message_size) {
  if (segment_info_.empty()) {
    AllocateSegment(topic_id, partition_id, 1);
  } else {
    SegmentInfo *last = segment_info_.back().get();
    if (!last->IsWriteable()) {
      AllocateSegment(topic_id, partition_id, last->GetSegmentId() + 1);
    } else if (!last->HasSpace(message_size)) {
      // TODO Close for writes
//      CloseForWrites(last);
      AllocateSegment(topic_id, partition_id, last->GetSegmentId() + 1);
    }
  }
  // TODO: Check message size <= segment_size
  return *segment_info_.back().get();
}
void BrokerNode::ReceiveAllocatedSegment(const UCP::Endpoint &endpoint,
                                         uint32_t topic_id,
                                         uint32_t partition_id,
                                         uint32_t segment_id) {
  uint32_t message_size;
  size_t received_length;
  ucs_status_ptr_t status_ptr = endpoint.receive(&message_size, sizeof(uint32_t), &received_length);
  ucs_status_t status = request_processor_.Process(status_ptr);
  if (!status == UCS_OK) {
    // TODO: Handle error
    throw std::runtime_error("Error!");
  }
  std::unique_ptr<char> buffer((char *) malloc(message_size));
  status_ptr = endpoint.receive(buffer.get(), message_size, &received_length);
  status = request_processor_.Process(status_ptr);
  if (!status == UCS_OK) {
    // TODO: Handle error
    throw ::std::runtime_error("Error!");
  }
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(buffer.get());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_Allocated: {
      auto allocated = static_cast<const Rembrandt::Protocol::Allocated *> (base_message->content());
      segment_info_.push_back(std::make_unique<SegmentInfo>(SegmentIdentifier{topic_id, partition_id, segment_id},
                                                            allocated->offset(),
                                                            allocated->size()));
      break;
    }
    case Rembrandt::Protocol::Message_AllocateFailed: {
      throw std::runtime_error("Not implemented!");
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
