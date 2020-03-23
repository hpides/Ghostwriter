#include <rembrandt/network/socket/static_client.h>
#include "rembrandt/network/connection_manager.h"

ConnectionManager::ConnectionManager(UCP::Worker &worker,
                                     UCP::EndpointFactory *endpoint_factory) :
    endpoint_factory_(endpoint_factory), worker_(worker) {};

UCP::Endpoint &ConnectionManager::GetConnection(char *server_addr, uint16_t port) {
  UCP::Endpoint *endpoint = findConnection(server_addr, port);
  if (endpoint == nullptr) {
    Connect(server_addr, port);
    endpoint = findConnection(server_addr, port);
  }
  return *endpoint;
}

UCP::Endpoint *ConnectionManager::findConnection(const char *server_addr, uint16_t port) const {
  auto it = connections_.find(std::pair(server_addr, port));
  if (it == connections_.end()) {
    return nullptr;
  } else {
    return it->second.get();
  }
}

void ConnectionManager::Connect(char *server_addr, uint16_t port) {
  connections_[std::pair(server_addr, port)] = std::move(endpoint_factory_->Create(worker_, server_addr, port));
}

void ConnectionManager::Disconnect(char *server_addr, uint16_t port) {
  connections_.erase(std::pair(server_addr, port));
}

void ConnectionManager::RegisterRemoteMemory(char *server_addr, uint16_t connection_port, uint16_t rkey_port) {
  UCP::Endpoint &endpoint = GetConnection(server_addr, connection_port);
  void *rkey_buffer = RequestRemoteKey(server_addr, rkey_port);
  endpoint.RegisterRKey(rkey_buffer);
}
void *ConnectionManager::RequestRemoteKey(char *server_addr, uint16_t rkey_port) const {
  StaticClient static_client = StaticClient();
  static_client.Connect(server_addr, rkey_port);
  void *rkey_buffer;
  size_t rkey_size;
  static_client.ReceivePayload(&rkey_buffer, &rkey_size);
  return rkey_buffer;
}
