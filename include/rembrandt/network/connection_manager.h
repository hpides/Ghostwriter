#ifndef REMBRANDT_SRC_NETWORK_CONNECTION_MANAGER_H_
#define REMBRANDT_SRC_NETWORK_CONNECTION_MANAGER_H_

#include <memory>
#include <unordered_map>
#include "boost/functional/hash.hpp"
#include "ucx/worker.h"
#include "ucx/endpoint_factory.h"
#include "request_processor.h"
class ConnectionManager {
 public:
  ConnectionManager(UCP::Worker &worker,
                    UCP::EndpointFactory *endpoint_factory,
                    MessageGenerator &message_generator_);
  UCP::Endpoint &GetConnection(const std::string &server_addr, uint16_t port);
  void Disconnect(char *server_addr, uint16_t port);
  void RegisterRemoteMemory(const std::string &server_addr, uint16_t connection_port, uint16_t rkey_port);
 private:
  UCP::EndpointFactory *endpoint_factory_;
  MessageGenerator &message_generator_;
  UCP::Worker &worker_;
  std::unordered_map<std::pair<std::string, uint16_t>,
                     std::unique_ptr<UCP::Endpoint>,
                     boost::hash<std::pair<std::string, uint16_t>>> connections_;
  void Connect(const std::string &server_addr, uint16_t port);
  void *RequestRemoteKey(const std::string &server_addr, uint16_t rkey_port) const;
  UCP::Endpoint *FindConnection(const std::string &server_addr, uint16_t port) const;
  void InitializeConnection(UCP::Endpoint &endpoint) const;
  void ReceiveInitialized(UCP::Endpoint &endpoint, RequestProcessor &request_processor) const;
};

#endif //REMBRANDT_SRC_NETWORK_CONNECTION_MANAGER_H_
