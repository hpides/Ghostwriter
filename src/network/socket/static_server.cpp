#include "../../../include/rembrandt/network/socket/static_server.h"

#include <cstring>
#include <iostream>
#include <stdexcept>
#include <cstdio>

extern "C" {
#include <arpa/inet.h>
#include <unistd.h>
}
StaticServer::StaticServer() : StaticServer(nullptr, 0) {}

StaticServer::StaticServer(std::shared_ptr<void> payload_buffer,
                           size_t payload_size)
    : payload_buffer_(payload_buffer),
      payload_length_(payload_size) {
  int ret;
  lsock_ = socket(AF_INET, SOCK_STREAM, 0);
  if (lsock_ < 0) {
    throw std::runtime_error("Failed opening server socket.");
  }
  int optval = 1;
  ret = setsockopt(lsock_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
  if (ret < 0) {
    throw std::runtime_error("Failed during setsockopt()");
  }
}

StaticServer::~StaticServer() {
  Close();
  close(lsock_);
//  TODO: RELEASE PAYLOAD
}

void StaticServer::Run(uint16_t port) {
  thread_ = std::thread(&StaticServer::Listen, this, port);
}

void StaticServer::Listen(uint16_t port) {
  if (running_) {
    throw std::runtime_error("Server is already running.");
  }
  struct sockaddr_in inaddr;
  int ret;

  port_ = port;

  inaddr.sin_family = AF_INET;
  inaddr.sin_port = htons(port_);
  inaddr.sin_addr.s_addr = INADDR_ANY;
  memset(inaddr.sin_zero, 0, sizeof(inaddr.sin_zero));
  ret = bind(lsock_, (struct sockaddr *) &inaddr, sizeof(inaddr));
  if (ret < 0) {
    close(lsock_);
    throw std::runtime_error("Failed binding server.");
  }

  ret = listen(lsock_, 0);
  if (ret < 0) {
    close(lsock_);
    throw std::runtime_error("Failed listening.");
  }

  running_ = true;
  fprintf(stdout, "Waiting for connection...\n");

  while (running_) {
    try {
      int oob_sock = AcceptConnection();
      SendPayload(oob_sock);
    } catch (const std::exception &e) {
      close(lsock_);
      throw e;
      // TODO: FIGURE OUT WHAT TO CLOSE
    }
  }
  std::cout << "Closing connections\n";
  // TODO: BARRIER
  close(lsock_);
}

int StaticServer::AcceptConnection() {
  int dsock = -1;
  int oob_sock;
/* Accept next connection */
  dsock = accept(lsock_, NULL, NULL);
  if (dsock < 0) {
    throw std::runtime_error("Failed accepting connection.");
  }
  oob_sock = dsock;
  if (oob_sock < 0) {
    throw std::runtime_error("Failed server setup.");
  }
  return oob_sock;
}

void StaticServer::SendPayload(int oob_sock) {
  int ret;
  ret = send(oob_sock, &payload_length_, sizeof(payload_length_), 0);
  if (ret != (int) sizeof(payload_length_)) {
    close(oob_sock);
    throw std::runtime_error("Failed sending payload length.");
  }
  ret = send(oob_sock, payload_buffer_.get(), payload_length_, 0);
  if (ret != (int) payload_length_) {
    close(oob_sock);
    throw std::runtime_error("Failed sending payload.");
  }
  // TODO: BARRIER
  close(oob_sock);
}

void StaticServer::SetPayload(std::shared_ptr<void> payload_buffer,
                              size_t payload_size) {
  payload_buffer_ = std::move(payload_buffer);
  payload_length_ = payload_size;
}

void StaticServer::Close() {
  running_ = false;
  std::cout << "Closing\n";
  thread_.join();
}
