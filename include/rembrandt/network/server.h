#ifndef REMBRANDT_SRC_NETWORK_SERVER_H_
#define REMBRANDT_SRC_NETWORK_SERVER_H_

#include "rembrandt/network/message.h"
#include "rembrandt/network/utils.h"
#include "rembrandt/network/socket/static_server.h"
#include "rembrandt/network/ucx/memory_region.h"
#include "rembrandt/network/ucx/endpoint.h"
#include "rembrandt/network/ucx/worker.h"
#include "message_handler.h"
#include <atomic>

#include <memory>

extern "C" {
#include <arpa/inet.h> /* inet_addr */
#include <ucp/api/ucp.h>
#include <ucs/type/status.h>
}

class Server {
 public:
  Server(UCP::Worker &data_worker, UCP::Worker &listening_worker, uint16_t port);
  void Listen();
  void Run(MessageHandler *message_handler);
  void CreateServerEndpoint(ucp_conn_request_h conn_request);
  std::unique_ptr<Message> ReceiveMessage(const UCP::Endpoint &endpoint);
 private:
  MessageHandler *message_handler_;
  ucp_listener_h ucp_listener_;
  UCP::Worker &data_worker_;
  UCP::Worker &listening_worker_;
  std::unordered_map<ucp_ep_h, std::unique_ptr<UCP::Endpoint>> endpoint_map_;
  std::thread listening_thread_;
  std::atomic<bool> running_ = false;
  static sockaddr_in CreateListenAddress(uint16_t port);
  ucp_listener_params_t CreateListenerParams(sockaddr_in *listen_addr);
  static ucp_ep_params_t CreateEndpointParams(ucp_conn_request_h conn_request);
  void StartListener(uint16_t port);
  ucs_status_t Finish(ucs_status_ptr_t status_ptr);
  std::deque<UCP::Endpoint *> WaitUntilReadyToReceive();
};

void server_conn_req_cb(ucp_conn_request_h conn_request, void *arg);

#endif //REMBRANDT_SRC_NETWORK_SERVER_H_
