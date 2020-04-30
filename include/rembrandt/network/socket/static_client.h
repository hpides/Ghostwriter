#ifndef REMBRANDT_SRC_NETWORK_STATIC_CLIENT_H_
#define REMBRANDT_SRC_NETWORK_STATIC_CLIENT_H_

#include <cstddef>
#include <string>

extern "C" {
#include <arpa/inet.h>
}

class StaticClient {
 private:
  struct sockaddr_in conn_addr_;
  int connfd_;
 public:
  StaticClient();
  void Connect(const std::string &address,
               uint16_t port);
  void ReceivePayload(void **payload_buffer,
                      size_t *payload_size);
  void Disconnect();
};

#endif //REMBRANDT_SRC_NETWORK_STATIC_CLIENT_H_
