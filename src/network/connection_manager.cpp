#include <rembrandt/network/socket/static_client.h>
#include <iostream>
#include "rembrandt/network/connection_manager.h"

ConnectionManager::ConnectionManager(UCP::Worker &worker,
                                     UCP::EndpointFactory *endpoint_factory,
                                     MessageGenerator &message_generator) :
    endpoint_factory_(endpoint_factory), message_generator_(message_generator), worker_(worker) {}

UCP::Endpoint &ConnectionManager::GetConnection(const std::string &server_addr, uint16_t port) {
  UCP::Endpoint *endpoint = FindConnection(server_addr, port);
  if (endpoint == nullptr) {
    Connect(server_addr, port);
    endpoint = FindConnection(server_addr, port);
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

void ConnectionManager::RegisterRemoteMemory(const std::string &server_addr,
                                             uint16_t connection_port,
                                             uint16_t rkey_port) {
  UCP::Endpoint &endpoint = GetConnection(server_addr, connection_port);
  void *rkey_buffer = RequestRemoteKey(server_addr, rkey_port);
  endpoint.RegisterRKey(rkey_buffer);
}
void *ConnectionManager::RequestRemoteKey(const std::string &server_addr, uint16_t rkey_port) const {
  // TODO: Inject static client for testing
  StaticClient static_client = StaticClient();
  static_client.Connect(server_addr, rkey_port);
  void *rkey_buffer;
  size_t rkey_size;
  static_client.ReceivePayload(&rkey_buffer, &rkey_size);
  static_client.Disconnect();
  return rkey_buffer;
}
void ConnectionManager::InitializeConnection(UCP::Endpoint &endpoint) const {
  RequestProcessor request_processor(worker_);
  std::unique_ptr<Message> message = message_generator_.Initialize();
  ucs_status_ptr_t ucs_status_ptr = endpoint.send(message->GetBuffer(), message->GetSize());
  ucs_status_t status = request_processor.Process(ucs_status_ptr);
  if (status != UCS_OK) {
    throw std::runtime_error("Failed sending initialization request!\n");
  }
  std::cout << "Sent initialization request!\n";
  ReceiveInitialized(endpoint, request_processor);
}

void ConnectionManager::ReceiveInitialized(UCP::Endpoint &endpoint, RequestProcessor &request_processor) const {
  uint32_t message_size;
  size_t received_length;
  ucs_status_ptr_t status_ptr = endpoint.receive(&message_size, sizeof(uint32_t), &received_length);
  ucs_status_t status = request_processor.Process(status_ptr);
  if (!status == UCS_OK) {
    // TODO: Handle error
    throw std::runtime_error("Error!");
  }
  std::unique_ptr<char> buffer((char *) malloc(message_size));
  status_ptr = endpoint.receive(buffer.get(), message_size, &received_length);
  status = request_processor.Process(status_ptr);
  if (!status == UCS_OK) {
    // TODO: Handle error
    throw ::std::runtime_error("Error!");
  }
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(buffer.get());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_Initialized: {
      std::cout << "Received initialization response!\n";
      break;
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}
