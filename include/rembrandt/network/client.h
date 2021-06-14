#ifndef REMBRANDT_SRC_NETWORK_CLIENT_H_
#define REMBRANDT_SRC_NETWORK_CLIENT_H_

#include "connection_manager.h"
#include <rembrandt/network/request_processor.h>
#include <rembrandt/network/ucx/worker.h>
#include <rembrandt/protocol/message_generator.h>
class Client {
public:
  Client(std::unique_ptr<ConnectionManager> connection_manager_p,
         std::unique_ptr<MessageGenerator> message_generator_p,
         std::unique_ptr<RequestProcessor> request_processor_p,
         std::unique_ptr<UCP::Worker> worker_p);
  virtual ~Client() = 0;

protected:
  std::unique_ptr<ConnectionManager> connection_manager_p_;
  std::unique_ptr<MessageGenerator> message_generator_p_;
  std::unique_ptr<RequestProcessor> request_processor_p_;
  std::unique_ptr<UCP::Worker> worker_p_;
  virtual UCP::Endpoint &GetEndpointWithRKey() const = 0;
  UCP::Endpoint &GetEndpointWithRKey(const std::string &server_addr,
                                     uint16_t port) const;
  void SendMessage(const Message &message, const UCP::Endpoint &endpoint);
  std::unique_ptr<char> ReceiveMessage(const UCP::Endpoint &endpoint);
  void WaitUntilReadyToReceive(const UCP::Endpoint &endpoint);
};

#endif // REMBRANDT_SRC_NETWORK_CLIENT_H_
