#include "rembrandt/broker/broker_node.h"
#include <iostream>

BrokerNode::BrokerNode(UCP::Context &context,
                       ConnectionManager &connection_manager,
                       MessageGenerator &message_generator,
                       RequestProcessor &request_processor,
                       UCP::Worker &data_worker,
                       UCP::Worker &listening_worker,
                       BrokerNodeConfig config)
    : MessageHandler(message_generator),
      config_(config),
      connection_manager_(connection_manager),
      request_processor_(request_processor),
      data_worker_(data_worker),
      listening_worker_(listening_worker),
      server_(context, data_worker, listening_worker, config.server_port) {}

void BrokerNode::Run() {
  server_.Run(this);
}

std::unique_ptr<Message> BrokerNode::HandleMessage(Message &raw_message) {
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
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}

std::unique_ptr<Message> BrokerNode::HandleCommitRequest(const Rembrandt::Protocol::BaseMessage *commit_request) {
  auto commit_data = static_cast<const Rembrandt::Protocol::Commit *> (commit_request->content());
  if (segment_info_->Commit(commit_data->offset())) {
    return message_generator_.Committed(commit_request, commit_data->offset());
  } else {
    return message_generator_.CommitFailed(commit_request);
  }
}
std::unique_ptr<Message> BrokerNode::HandleStageRequest(const Rembrandt::Protocol::BaseMessage *stage_request) {
  auto stage_data = static_cast<const Rembrandt::Protocol::Stage *> (stage_request->content());
  uint64_t message_size = stage_data->total_size();
  TopicPartition topic_partition = TopicPartition(stage_data->topic_id(), stage_data->partition_id());
  SegmentInfo &segment_info = GetSegmentInfo(topic_partition);
  // TODO: Adjust overwriting logic in Stage()
//  if (segment_info.HasSpace(message_size)) {
    uint64_t offset = segment_info.Stage(message_size);
    return message_generator_.Staged(stage_request, offset);
//  } else {
//    return message_generator_.StageFailed(stage_request);
//  }
}

SegmentInfo &BrokerNode::GetSegmentInfo(const TopicPartition &topic_partition) {
  if (!segment_info_) {
    AllocateSegment(topic_partition);
  }
  return *segment_info_;
}

void BrokerNode::AllocateSegment(const TopicPartition &topic_partition) {
  std::unique_ptr<Message> allocate_message = message_generator_.Allocate(topic_partition);
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.storage_node_ip,
                                                              config_.storage_node_port);
  SendMessage(*allocate_message, endpoint);
  WaitUntilReadyToReceive(endpoint);
  ReceiveAllocatedSegment(endpoint, topic_partition);
}

void BrokerNode::ReceiveAllocatedSegment(UCP::Endpoint &endpoint, const TopicPartition &topic_partition) {
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
      segment_info_ = std::make_unique<SegmentInfo>(topic_partition, allocated->offset(), allocated->size());
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

void BrokerNode::SendMessage(Message &message, UCP::Endpoint &endpoint) {
  ucs_status_ptr_t ucs_status_ptr = endpoint.send(message.GetBuffer(), message.GetSize());
  ucs_status_t status = request_processor_.Process(ucs_status_ptr);
  if (status != UCS_OK) {
    throw std::runtime_error("Failed sending request!\n");
  }
  // TODO: Adjust to handling different response types
}

void BrokerNode::WaitUntilReadyToReceive(UCP::Endpoint &endpoint) {
  ucp_stream_poll_ep_t *stream_poll_eps = (ucp_stream_poll_ep_t *) malloc(sizeof(ucp_stream_poll_ep_t) * 5);
  while (true) {
    ssize_t num_eps = ucp_stream_worker_poll(data_worker_.GetWorkerHandle(), stream_poll_eps, 5, 0);
    if (num_eps > 0) {
      if (stream_poll_eps->ep == endpoint.GetHandle()) {
        break;
      }
    } else if (num_eps < 0) {
      throw std::runtime_error("Error!");
    } else {
      data_worker_.Progress();
    }
  }
  free(stream_poll_eps);
}
