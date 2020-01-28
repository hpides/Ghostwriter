#ifndef REMBRANDT_SRC_NETWORK_CLIENT_H_
#define REMBRANDT_SRC_NETWORK_CLIENT_H_

#include <arpa/inet.h> /* inet_addr */

extern "C" {
#include <ucp/api/ucp.h>
}

class Client {
 public:
  ucp_context_h &ucp_context_;
  ucp_worker_h &ucp_worker_;
  Client(ucp_context_h &ucp_context,
         ucp_worker_h &ucp_worker);
  ~Client();
  ucp_ep_h Connect(char *server_addr,
                   uint16_t port = 13337);
  void Disconnect(ucp_ep_h &ep);
  void SendTest(ucp_ep_h &ep);
 private:
  struct sockaddr_in CreateConnectionAddress(const char *address,
                                             const uint16_t port);
  ucp_ep_params_t CreateParams(struct sockaddr_in &connect_addr);
};

#endif //REMBRANDT_SRC_NETWORK_CLIENT_H_
