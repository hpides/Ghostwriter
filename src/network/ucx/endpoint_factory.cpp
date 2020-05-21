#include <cstring>
#include <rembrandt/network/request_processor.h>
#include <assert.h>
#include <iostream>
#include "rembrandt/network/ucx/endpoint_factory.h"

using namespace UCP;

std::unique_ptr<Endpoint> EndpointFactory::Create(Worker &worker, const std::string &server_addr, uint16_t port) const {
  struct sockaddr_in connect_addr = CreateConnectionAddress(server_addr, port);
  const ucp_ep_params_t params = CreateParams(connect_addr);
  return std::make_unique<UCP::Impl::Endpoint>(worker, &params);
}

struct sockaddr_in EndpointFactory::CreateConnectionAddress(const std::string &address, uint16_t port) {
  struct sockaddr_in connect_addr;
  memset(&connect_addr, 0, sizeof(struct sockaddr_in));
  connect_addr.sin_family = AF_INET;
  connect_addr.sin_addr.s_addr = inet_addr(address.c_str());
  connect_addr.sin_port = htons(port);
  return connect_addr;
}

ucp_ep_params_t EndpointFactory::CreateParams(struct sockaddr_in &connect_addr) {
  ucp_ep_params_t params;
  params.field_mask = UCP_EP_PARAM_FIELD_FLAGS |
      UCP_EP_PARAM_FIELD_SOCK_ADDR |
      UCP_EP_PARAM_FIELD_ERR_HANDLER;
  params.err_handler.cb = err_cb;
  params.err_handler.arg = nullptr;
  params.flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
  params.sockaddr.addr = (struct sockaddr *) &connect_addr;
  params.sockaddr.addrlen = sizeof(connect_addr);
  return params;
}

/**
 * Error handling callback.
 */
static void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status) {
  printf("error handling callback was invoked with status %d (%s)\n",
         status, ucs_status_string(status));
}
