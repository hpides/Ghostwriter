#ifndef REMBRANDT_SRC_NETWORK_UCX_ENDPOINT_FACTORY_H_
#define REMBRANDT_SRC_NETWORK_UCX_ENDPOINT_FACTORY_H_

#include <memory>

#include <arpa/inet.h> /* inet_addr */
#include <rembrandt/protocol/message_generator.h>
#include <rembrandt/network/request_processor.h>
#include "endpoint.h"
#include "worker.h"

namespace UCP {
class EndpointFactory {
 public:
  EndpointFactory(MessageGenerator &message_generator);
  virtual std::unique_ptr<Endpoint> Create(Worker &worker, char *server_addr, uint16_t port);
  void InitializeConnection(Endpoint &endpoint, Worker &worker);
 private:
  MessageGenerator &message_generator_;
  static struct sockaddr_in CreateConnectionAddress(const char *address, const uint16_t);
  static ucp_ep_params_t CreateParams(struct sockaddr_in &connect_addr);
  void ReceiveInitialized(UCP::Endpoint &endpoint, RequestProcessor &request_processor);
};
}

static void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status);

#endif //REMBRANDT_SRC_NETWORK_UCX_ENDPOINT_FACTORY_H_
