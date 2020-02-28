#ifndef REMBRANDT_SRC_NETWORK_UCX_CLIENT_H_
#define REMBRANDT_SRC_NETWORK_UCX_CLIENT_H_

#include <memory>
#include <unordered_map>
#include <arpa/inet.h> /* inet_addr */
#include "endpoint.h"
#include "boost/functional/hash.hpp"

#include "../../utils.h"
#include "context.h"
#include "worker.h"

extern "C" {
#include "ucp/api//ucp.h"
}

namespace UCP {
// This class is not thread-safe!
class Client {
 public:
  Client(Context &context);
  ~Client();
  Endpoint &GetConnection(char *server_addr, uint16_t port);
  void Disconnect(char *server_addr, uint16_t port);
  ucp_rkey_h RegisterRemoteMemory(Endpoint &ep,
                                  char *server_addr,
                                  __uint16_t rkey_port = 13338);
  void SendTest(ucp_ep_h &ep);
  Worker &GetWorker() { return worker_; };
 private:
  Context &context_;
  Worker worker_;
  std::unordered_map<std::pair<std::string, uint16_t>,
                     std::shared_ptr<Endpoint>,
                     boost::hash<std::pair<std::string, uint16_t>>> endpoints_;
  struct sockaddr_in CreateConnectionAddress(const char *address,
                                             const uint16_t port);
  ucp_ep_params_t CreateParams(struct sockaddr_in &connect_addr);
  void Connect(char *server_addr,
               uint16_t port = 13337);
};
}

#endif //REMBRANDT_SRC_NETWORK_UCX_CLIENT_H_
