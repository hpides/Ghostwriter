#include <cstring>
#include <rembrandt/network/request_processor.h>
#include <assert.h>
#include "rembrandt/network/ucx/endpoint_factory.h"

using namespace UCP;
std::unique_ptr<Endpoint> EndpointFactory::Create(Worker &worker, char *server_addr, uint16_t port) {
  struct sockaddr_in connect_addr = CreateConnectionAddress(server_addr, port);
  const ucp_ep_params_t params = CreateParams(connect_addr);
  std::unique_ptr<Endpoint> endpoint = std::make_unique<Endpoint>(worker, &params);
  InitializeConnection(*endpoint, worker);
  return std::move(endpoint);
}

void EndpointFactory::InitializeConnection(UCP::Endpoint &endpoint, UCP::Worker &worker) {
  char init[] = "init";
  RequestProcessor request_processor(worker);
  ucs_status_ptr_t status_ptr = endpoint.send(init, sizeof(init));
  ucs_status_t status = request_processor.Process(status_ptr);
  if (status != UCS_OK) {
    throw std::runtime_error("Failed sending initialization request!\n");
  }
  char recv[sizeof(init)] = "";
  size_t received_length;
  status_ptr = endpoint.receive(&recv, sizeof(init), &received_length);
  status = request_processor.Process(status_ptr);
  if (status != UCS_OK) {
    throw std::runtime_error("Failed receiving initialization response!\n");
  }
  assert(std::strcmp(recv, init) == 0);
}

struct sockaddr_in EndpointFactory::CreateConnectionAddress(const char *address, const uint16_t port) {
  struct sockaddr_in connect_addr;
  memset(&connect_addr, 0, sizeof(struct sockaddr_in));
  connect_addr.sin_family = AF_INET;
  connect_addr.sin_addr.s_addr = inet_addr(address);
  connect_addr.sin_port = htons(port);
  return connect_addr;
}

ucp_ep_params_t EndpointFactory::CreateParams(struct sockaddr_in &connect_addr) {
  ucp_ep_params_t params;
  params.field_mask = UCP_EP_PARAM_FIELD_FLAGS |
      UCP_EP_PARAM_FIELD_SOCK_ADDR |
      UCP_EP_PARAM_FIELD_ERR_HANDLER |
      UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
  params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
  params.err_handler.cb = err_cb;
  params.err_handler.arg = NULL;
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
