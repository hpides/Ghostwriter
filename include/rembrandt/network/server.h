#ifndef REMBRANDT_SRC_NETWORK_SERVER_H_
#define REMBRANDT_SRC_NETWORK_SERVER_H_

#include "rembrandt/network/message.h"
#include "rembrandt/network/utils.h"
#include "rembrandt/network/socket/static_server.h"
#include "rembrandt/network/ucx/memory_region.h"
#include "rembrandt/network/ucx/context.h"
#include "rembrandt/network/ucx/endpoint.h"
#include "rembrandt/network/ucx/worker.h"
#include "message_handler.h"

#include <memory>

extern "C" {
#include <arpa/inet.h> /* inet_addr */
#include <ucp/api/ucp.h>
#include <ucs/type/status.h>
}

class Server {
 public:
  Server(UCP::Context &context,
         uint16_t port = 13337);

  void Listen(MessageHandler *message_handler);
  void CreateServerEndpoint(ucp_conn_request_h conn_request);
  Message ReceiveMessage();
 private:
  MessageHandler *message_handler_;
  UCP::Context &context_;
  ucp_listener_h ucp_listener_;
  ucx_server_ctx_t server_context_;
  UCP::Worker worker_;
  std::unique_ptr<UCP::Endpoint> endpoint_;
  sockaddr_in CreateListenAddress(uint16_t port);
  ucp_listener_params_t CreateListenerParams(sockaddr_in *listen_addr);
  ucp_ep_params_t CreateEndpointParams(ucp_conn_request_h conn_request);
  void StartListener(uint16_t port);
  ucs_status_t Finish(ucs_status_ptr_t status_ptr);
};

static ucs_status_t request_wait(ucp_worker_h ucp_worker, test_req_t *request);

static void server_conn_req_cb(ucp_conn_request_h conn_request, void *arg);

#endif //REMBRANDT_SRC_NETWORK_SERVER_H_
