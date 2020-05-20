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
               ProducerConfig &config) : Client(connection_manager,
                                                message_generator,
                                                request_processor,
                                                worker),
                                         config_(config) {}
void Sender::Send(Batch *batch) {
  uint64_t offset = Stage(batch);
  Store(batch, offset);
  Commit(batch, offset);
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
  return Client::GetEndpointWithRKey(config_.storage_node_ip,
                                     config_.storage_node_port);
}

uint64_t Sender::Stage(Batch *batch) {
  std::unique_ptr<Message> stage_message = message_generator_.Stage(batch);
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.broker_node_ip, config_.broker_node_port);
  SendMessage(*stage_message, endpoint);
  WaitUntilReadyToReceive(endpoint);
  return ReceiveStagedOffset(endpoint);
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
  std::unique_ptr<char> buffer = Client::ReceiveMessage(endpoint);
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
