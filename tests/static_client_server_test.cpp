#include <thread>
#include "../include/rembrandt/network/socket/static_client.h"
#include "../include/rembrandt/network/socket/static_server.h"
#include "gtest/gtest.h"

#include <memory>
#include <string>
TEST(StaticClientServer, DataExchange) {
  std::shared_ptr<void> payload_ptr = std::shared_ptr<char>((char *) ("Lorem ipsum dolor sit amet."));
  size_t payload_size = strlen((char *) payload_ptr.get());
  StaticServer
      *static_server = new StaticServer(payload_ptr, payload_size);
  std::thread th(&StaticServer::Listen, static_server, 13338);
  // TODO: REFACTOR CLIENT/SERVER TO USE NON-BLOCKING ACCEPT AND ENABLE RETRIES / SHUTDOWN
  sleep(1);
  StaticClient static_client = StaticClient();
  static_client.Connect((char *) "127.0.0.1", 13338);
  void *recv_payload;
  size_t recv_size;
  sleep(1);
  static_client.ReceivePayload(&recv_payload, &recv_size);
  sleep(1);
  static_client.Disconnect();
  ASSERT_EQ(payload_size, recv_size);
  ASSERT_STREQ((char *) payload_ptr.get(), (char *) recv_payload);
  ASSERT_STREQ((char *) recv_payload, "Lorem ipsum dolor sit amet.");
  static_server->Close();
  th.join();
  // TODO: GRACEFUL SHUTDOWN
}