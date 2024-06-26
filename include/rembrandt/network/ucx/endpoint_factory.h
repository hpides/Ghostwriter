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
  virtual std::unique_ptr<Endpoint> Create(Worker &worker, const std::string &server_addr, uint16_t port) const;
 private:
  static ucp_address_t *FetchAddress(const std::string &address, uint16_t port, Worker &worker);
  static ucp_ep_params_t CreateParams(ucp_address_t *connect_addr);
};
}

void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status);

#endif //REMBRANDT_SRC_NETWORK_UCX_ENDPOINT_FACTORY_H_
