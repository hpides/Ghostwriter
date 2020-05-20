#ifndef REMBRANDT_SRC_NETWORK_CLIENT_H_
#define REMBRANDT_SRC_NETWORK_CLIENT_H_

#include <rembrandt/network/ucx/worker.h>
#include <rembrandt/protocol/message_generator.h>
#include <rembrandt/network/request_processor.h>
#include "connection_manager.h"
class Client {
 public:
  Client(ConnectionManager &connection_manager,
         MessageGenerator &message_generator,
         RequestProcessor &request_processor,
         UCP::Worker &worker);
  virtual ~Client() = 0;
 protected:
  ConnectionManager &connection_manager_;
  MessageGenerator &message_generator_;
  RequestProcessor &request_processor_;
  UCP::Worker &worker_;
  virtual UCP::Endpoint &GetEndpointWithRKey() const = 0;
  UCP::Endpoint &GetEndpointWithRKey(const std::string &server_addr, uint16_t port) const;
  void SendMessage(const Message &message, const UCP::Endpoint &endpoint);
  std::unique_ptr<char> ReceiveMessage(const UCP::Endpoint &endpoint);
  void WaitUntilReadyToReceive(const UCP::Endpoint &endpoint);
};

#endif //REMBRANDT_SRC_NETWORK_CLIENT_H_
