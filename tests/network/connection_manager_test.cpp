#include "rembrandt/network/connection_manager.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "rembrandt/network/ucx/worker.h"
#include "rembrandt/network/ucx/endpoint_factory.h"

namespace {
class MockEndpointFactory : public UCP::EndpointFactory {
 public:
  MockEndpointFactory(MessageGenerator &message_generator) : UCP::EndpointFactory(message_generator) {};
  MOCK_METHOD(std::unique_ptr<UCP::Endpoint>, Create, (UCP::Worker & worker, char * server_addr, uint16_t
      port), (const, override));
};

class ConnectionManagerTest : public testing::Test {
 public:
  ConnectionManagerTest()
      : context_(false),
        worker_(context_),
        message_generator_(),
        mock_endpoint_factory_(message_generator_),
        connection_manager_(worker_, &mock_endpoint_factory_) {};
 protected:
  UCP::Context context_;
  UCP::Worker worker_;
  MessageGenerator message_generator_;
  MockEndpointFactory mock_endpoint_factory_;
  ConnectionManager connection_manager_;
};

TEST_F(ConnectionManagerTest, GetNewConnection) {
  EXPECT_CALL(mock_endpoint_factory_, Create(::testing::Ref(worker_), (char *) "localhost", 1234)).Times(1);
  connection_manager_.GetConnection("localhost", 1234);
}

TEST_F(ConnectionManagerTest, GetKnownConnection) {
  // TODO: Fix second call by returning mocked pointer instead of nullptr
  EXPECT_CALL(mock_endpoint_factory_, Create(::testing::Ref(worker_), (char *) "localhost", 1234)).Times(1);
  connection_manager_.GetConnection("localhost", 1234);
  connection_manager_.GetConnection("localhost", 1234);
}
}