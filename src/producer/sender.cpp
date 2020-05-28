#include <iostream>
#include <stdexcept>
#include <rembrandt/network/request_processor.h>
#include <rembrandt/network/detached_message.h>
#include "../../include/rembrandt/network/utils.h"
#include "../../include/rembrandt/network/ucx/endpoint.h"
#include "../../include/rembrandt/producer/sender.h"

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
  std::unique_ptr<Message> stage_message = message_generator_.StageMessageRequest(batch);
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.broker_node_ip, config_.broker_node_port);
  SendMessage(*stage_message, endpoint);
  std::unique_ptr<char> buffer = Client::ReceiveMessage(endpoint);
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(buffer.get());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_StageMessageResponse: {
      auto staged = static_cast<const Rembrandt::Protocol::StageMessageResponse *> (base_message->content());
      return staged->offset();
    }
    case Rembrandt::Protocol::Message_StageMessageException: {
      throw std::runtime_error("Handling StageMessageException not implemented!");
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}

bool Sender::Commit(Batch *batch, uint64_t offset) {
  std::unique_ptr<Message> commit_message = message_generator_.CommitRequest(batch->getTopic(),
                                                                             batch->getPartition(),
                                                                             offset);
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.broker_node_ip,
                                                              config_.broker_node_port);
  SendMessage(*commit_message, endpoint);
  std::unique_ptr<char> buffer = Client::ReceiveMessage(endpoint);
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(buffer.get());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_CommitResponse: {
      return true;
    }
    case Rembrandt::Protocol::Message_CommitException: {
      throw std::runtime_error("CommitRequest failed.");
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}
