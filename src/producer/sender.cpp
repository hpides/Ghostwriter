#include <iostream>
#include <stdexcept>
#include <rembrandt/network/request_processor.h>
#include <rembrandt/network/detached_message.h>
#include "../../include/rembrandt/network/utils.h"
#include "../../include/rembrandt/network/ucx/endpoint.h"
#include "../../include/rembrandt/producer/sender.h"
#include "../../include/rembrandt/producer/message_accumulator.h"

Sender::Sender(ConnectionManager &connection_manager,
               MessageGenerator &message_generator,
               RequestProcessor &request_processor,
               UCP::Worker &worker,
               ProducerConfig &config) : config_(config),
                                         connection_manager_(connection_manager),
                                         message_generator_(message_generator),
                                         request_processor_(request_processor),
                                         worker_(worker) {}

void Sender::Send(Batch *batch) {
  uint64_t offset = Stage(batch);
  Store(batch, offset);
//  Commit(batch, offset);
}

void Sender::Store(Batch *batch, uint64_t offset) {
  UCP::Endpoint &endpoint = GetEndpointWithRKey();

  ucs_status_ptr_t status_ptr = endpoint.put(batch->getBuffer(),
                                             batch->getSize(),
                                             endpoint.GetRemoteAddress() + offset,
                                             empty_cb);
  ucs_status_t status = request_processor_.Process(status_ptr);

  if (status != UCS_OK) {
    throw std::runtime_error("Failed storing batch!\n");
  }
}
UCP::Endpoint &Sender::GetEndpointWithRKey() const {
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.storage_node_ip, config_.storage_node_port);
  if (!endpoint.hasRKey()) {
    connection_manager_.RegisterRemoteMemory(config_.storage_node_ip,
                                             config_.storage_node_port,
                                             config_.storage_node_rkey_port);
  }
  return endpoint;
}

uint64_t Sender::Stage(Batch *batch) {
  std::unique_ptr<Message> stage_message = message_generator_.Stage(batch);
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.broker_node_ip, config_.broker_node_port);
  SendMessage(*stage_message, endpoint);
  WaitUntilReadyToReceive(endpoint);
  return ReceiveStagedOffset(endpoint);
}

void Sender::SendMessage(const Message &message, const UCP::Endpoint &endpoint) {
  ucs_status_ptr_t ucs_status_ptr = endpoint.send(message.GetBuffer(), message.GetSize());
  ucs_status_t status = request_processor_.Process(ucs_status_ptr);
  if (status != UCS_OK) {
    throw std::runtime_error("Failed sending request!\n");
  }
  // TODO: Adjust to handling different response types
}

void Sender::WaitUntilReadyToReceive(const UCP::Endpoint &endpoint) {
  ucp_stream_poll_ep_t *stream_poll_eps = (ucp_stream_poll_ep_t *) malloc(sizeof(ucp_stream_poll_ep_t) * 5);
  while (true) {
    ssize_t num_eps = ucp_stream_worker_poll(worker_.GetWorkerHandle(), stream_poll_eps, 5, 0);
    if (num_eps > 0) {
      if (stream_poll_eps->ep == endpoint.GetHandle()) {
        break;
      }
    } else if (num_eps < 0) {
      throw std::runtime_error("Error!");
    } else {
      worker_.Progress();
    }
  }
  free(stream_poll_eps);
}
uint64_t Sender::ReceiveStagedOffset(const UCP::Endpoint &endpoint) {
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
    case Rembrandt::Protocol::Message_Staged: {
      auto staged = static_cast<const Rembrandt::Protocol::Staged *> (base_message->content());
      return staged->offset();
    }
    case Rembrandt::Protocol::Message_StageFailed: {
      throw std::runtime_error("Not implemented!");
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}

bool Sender::Commit(Batch *batch, uint64_t offset) {
  std::unique_ptr<Message> commit_message = message_generator_.Commit(batch, offset);
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.broker_node_ip,
                                                              config_.broker_node_port);
  SendMessage(*commit_message, endpoint);
  return ReceiveCommitResponse(endpoint);
}

bool Sender::ReceiveCommitResponse(const UCP::Endpoint &endpoint) {
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
    case Rembrandt::Protocol::Message_Committed: {
      return true;
    }
    case Rembrandt::Protocol::Message_CommitFailed: {
      return false;
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}
