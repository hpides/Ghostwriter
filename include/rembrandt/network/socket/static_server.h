#ifndef REMBRANDT_SRC_NETWORK_STATIC_SERVER_H_
#define REMBRANDT_SRC_NETWORK_STATIC_SERVER_H_

#include <cstdint>
#include <cstring>
#include <memory>
#include <thread>

class StaticServer {
 private:
  int lsock_ = -1;
  uint16_t port_;
  bool running_ = false;
  std::shared_ptr<void> payload_buffer_;
  size_t payload_length_;
  std::thread thread_;
  int AcceptConnection();
  void SendPayload(int oob_sock);
 public:
  StaticServer();
  StaticServer(std::shared_ptr<void> payload_buffer,
               size_t payload_size);
  ~StaticServer();
  void SetPayload(std::shared_ptr<void> payload_buffer, size_t payload_size);
  uint16_t GetPort() { return port_; };
  void Listen(uint16_t port);
  void Run(uint16_t port);
  void Close();
};

#endif //REMBRANDT_SRC_NETWORK_STATIC_SERVER_H_
