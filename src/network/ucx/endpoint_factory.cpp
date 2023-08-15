#include <cstring>
#include <rembrandt/network/request_processor.h>
#include <assert.h>
#include <iostream>
#include "rembrandt/network/ucx/endpoint_factory.h"

using namespace UCP;

std::unique_ptr<Endpoint> EndpointFactory::Create(Worker &worker, const std::string &server_addr, uint16_t port) const {
  ucp_address_t *addr = FetchAddress(server_addr, port);
  const ucp_ep_params_t params = CreateParams(addr);
  return std::make_unique<UCP::Impl::Endpoint>(worker, &params);

ucp_address_t *EndpointFactory::FetchAddress(const std::string &address, uint16_t port) {
  struct sockaddr_in connect_addr;

  connfd = socket(AF_INET, SOCK_STREAM, 0);
  if (connfd < 0) {
    throw std::runtime_error("error: open client socket");
  }
  memset(&connect_addr, 0, sizeof(struct sockaddr_in));
  connect_addr.sin_family = AF_INET;
  connect_addr.sin_addr.s_addr = inet_addr(address.c_str());
  connect_addr.sin_port = htons(port);

  ret = connect(connfd, (struct sockaddr*) &connect_addr, sizeof(connect_addr));
  if (ret < 0) {
    throw std::runtime_error("connect client")
  }

  addr_len = ;
  ret = send(connfd, &addr_len, sizeof(addr_len), 0);
  if (ret != (int) sizeof(addr_len)) {
    throw std::runtime_error("send client address length");
  }
  ret = send(connfd, &addr, addr_len, 0);
  if (ret != (int) addr_len) {
    throw std::runtime_error("send client address");
  }

  size_t server_addr_len;
  ret = recv(connfd, &server_addr_len, sizeof(server_addr_len), MSG_WAITALL);
  if (ret != (int) sizeof(server_addr_len)) {
    throw std::runtime_error("recv server address length");
  }

  # FIXME: Free memory
  ucp_address_t *server_addr_p = malloc(server_addr_len);
  ret = recv(connfd, server_addr_p, server_addr_len, MSG_WAITALL);
  if (ret != (int) server_addr_len) {
    throw std::runtime_error("recv server address");
  }
  
  return server_addr_p;
}

ucp_ep_params_t EndpointFactory::CreateParams(ucp_address_t *addr) {
  ucp_ep_params_t params;
  params.field_mask = UCP_EP_PARAM_FIELD_FLAGS |
      UCP_EP_PARAM_FIELD_SOCK_ADDR |
      UCP_EP_PARAM_FIELD_ERR_HANDLER |
      UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
  params.err_handler.cb = err_cb;
  params.err_handler.arg = nullptr;
  params.address = addr;
  return params; 
     

/**
 * Error handling callback.
 */
void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status) {
  printf("error handling callback was invoked with status %d (%s)\n",
         status, ucs_status_string(status));
}
