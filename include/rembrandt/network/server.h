#ifndef REMBRANDT_SRC_NETWORK_SERVER_H_
#define REMBRANDT_SRC_NETWORK_SERVER_H_

#include "rembrandt/network/utils.h"
#include "rembrandt/network/socket/static_server.h"
#include "rembrandt/network/ucx/memory_region.h"
#include "rembrandt/network/ucx/context.h"
#include "rembrandt/network/ucx/endpoint.h"
#include "rembrandt/network/ucx/worker.h"

#include <memory>

extern "C" {
#include <arpa/inet.h> /* inet_addr */
#include <ucp/api/ucp.h>
#include <ucs/type/status.h>
}

class Server {
 public:
  Server(UCP::Context &context,
         uint16_t port = 13337,
         uint16_t rkey_port = 13338);

  void Listen();
  void Listen(ucp_stream_recv_callback_t recv_cb);
  void CreateServerEndpoint(ucp_conn_request_h conn_request);
 private:
  StaticServer rkey_server_;
  UCP::Context &context_;
  ucp_listener_h ucp_listener_;
  ucx_server_ctx_t server_context_;
  UCP::Worker worker_;
  std::unique_ptr<UCP::Endpoint> endpoint_;
  UCP::MemoryRegion memory_region_;
  sockaddr_in CreateListenAddress(uint16_t port);
  ucp_listener_params_t CreateListenerParams(sockaddr_in *listen_addr);
  ucp_ep_params_t CreateEndpointParams(ucp_conn_request_h conn_request);
  void StartRKeyServer(uint16_t port);
  void StartListener(uint16_t port);
};

static ucs_status_t request_wait(ucp_worker_h ucp_worker, test_req_t *request);

static int receive_stream(ucp_worker_h ucp_worker,
                          ucp_ep_h ep);

static void stream_recv_cb(void *request, ucs_status_t status, size_t length);

static void server_conn_req_cb(ucp_conn_request_h conn_request, void *arg);

#endif //REMBRANDT_SRC_NETWORK_SERVER_H_
