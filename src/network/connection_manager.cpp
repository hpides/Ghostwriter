#include <iostream>
#include "rembrandt/network/connection_manager.h"

ConnectionManager::ConnectionManager(UCP::Worker &worker,
                                     UCP::EndpointFactory *endpoint_factory,
                                     MessageGenerator &message_generator_,
                                     RequestProcessor &request_processor) :
    endpoint_factory_(endpoint_factory),
    message_generator_(message_generator_),
    request_processor_(request_processor),
    worker_(worker) {}

UCP::Endpoint &ConnectionManager::GetConnection(const std::string &server_addr, uint16_t port, bool rdma_enabled) {
  UCP::Endpoint *endpoint = FindConnection(server_addr, port);
  if (endpoint == nullptr) {
    Connect(server_addr, port);
    endpoint = FindConnection(server_addr, port);
  }
  if (rdma_enabled && !endpoint->hasRKey()) {
    RegisterRemoteMemory(server_addr, port);
  }
  return *endpoint;
}

UCP::Endpoint *ConnectionManager::FindConnection(const std::string &server_addr, uint16_t port) const {
  auto it = connections_.find(std::pair(server_addr, port));
  if (it == connections_.end()) {
    return nullptr;
  } else {
    return it->second.get();
  }
}

void ConnectionManager::Connect(const std::string &server_addr, uint16_t port) {
  std::unique_ptr<UCP::Endpoint> endpoint = endpoint_factory_->Create(worker_, server_addr, port);
  InitializeConnection(*endpoint);
  connections_[std::pair(server_addr, port)] = std::move(endpoint);
}

void ConnectionManager::Disconnect(char *server_addr, uint16_t port) {
  connections_.erase(std::pair(server_addr, port));
}

void ConnectionManager::RegisterRemoteMemory(const std::string &server_addr, uint16_t connection_port) {
  UCP::Endpoint &endpoint = GetConnection(server_addr, connection_port);
  std::unique_ptr<Message> message = message_generator_.RMemInfoRequest();
  SendMessage(*message, endpoint);
  std::unique_ptr<char> buffer = ReceiveMessage(endpoint);
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(buffer.get());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_RMemInfoResponse: {
      std::cout << "Received Remote Memory Information!\n";
      auto rmem_info = static_cast<const Rembrandt::Protocol::RMemInfoResponse *> (base_message->content());
      endpoint.RegisterRMemInfo(rmem_info->remote_key()->str(), rmem_info->remote_address());
      break;
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}
void ConnectionManager::InitializeConnection(UCP::Endpoint &endpoint) const {
  std::unique_ptr<Message> message = message_generator_.InitializeRequest();
  SendMessage(*message, endpoint);
  ReceiveInitializeResponse(endpoint);
}

void ConnectionManager::ReceiveInitializeResponse(UCP::Endpoint &endpoint) const {
  std::unique_ptr<char> buffer = ReceiveMessage(endpoint);
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(buffer.get());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_InitializeResponse: {
      std::cout << "Received InitializeResponse!\n";
      break;
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}

void ConnectionManager::SendMessage(const Message &message, const UCP::Endpoint &endpoint) const {
  ucs_status_ptr_t ucs_status_ptr = endpoint.send(message.GetBuffer(), message.GetSize());
  ucs_status_t status = request_processor_.Process(ucs_status_ptr);
  if (status != UCS_OK) {
    throw std::runtime_error("Failed sending request!\n");
  }
  // TODO: Adjust to handling different response types
}

std::unique_ptr<char> ConnectionManager::ReceiveMessage(const UCP::Endpoint &endpoint) const {
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
  return buffer;
}
