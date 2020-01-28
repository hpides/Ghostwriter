#include "../../../include/rembrandt/network/socket/static_client.h"

#include <cstring>
#include <stdexcept>
#include <unistd.h>
#include <netdb.h>

StaticClient::StaticClient() {
  connfd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (connfd_ < 0) {
    throw std::runtime_error("Failed to open client socket.");
  }
}
void StaticClient::Connect(char *address, uint16_t port) {
  struct hostent *he = gethostbyname(address);
  if (he == NULL || he->h_addr_list == NULL) {
    close(connfd_);
    throw std::runtime_error("Failed to find the host.");
  }

  conn_addr_.sin_family = he->h_addrtype;
  conn_addr_.sin_port = htons(port);

  memcpy(&conn_addr_.sin_addr, he->h_addr_list[0], he->h_length);
  memset(conn_addr_.sin_zero, 0, sizeof(conn_addr_.sin_zero));

  int ret = connect(connfd_,
                    (struct sockaddr *) &conn_addr_,
                    sizeof(conn_addr_));

  if (ret < 0) {
    close(connfd_);
    throw std::runtime_error("Failed to connect client.");
  }
}
void StaticClient::ReceivePayload(void **payload_buffer,
                                  size_t *payload_size) {
// TODO: Do correctly
  int ret = recv(connfd_, payload_size, sizeof(*payload_size), MSG_WAITALL);
  if (ret != (int) sizeof(payload_size)) {
    throw std::runtime_error("Failed to receive payload length.");
  }
  *payload_buffer = malloc(*payload_size);
  ret = recv(connfd_, *payload_buffer, *payload_size, MSG_WAITALL);
  if (ret != (int) *payload_size) {
// TODO: Exclusive PTR
    free(payload_buffer);
    throw std::runtime_error("Failed to receive payload.");
  }
}

void StaticClient::Disconnect() {
  // TODO: BARRIER
  close(connfd_);
}
