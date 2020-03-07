#include "../include/rembrandt/network/connection_manager.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "../include/rembrandt/network/ucx/worker.h"
#include "../include/rembrandt/network/ucx/endpoint_factory.h"

class MockEndpointFactory : public UCP::EndpointFactory {
 public:
  MOCK_METHOD(std::unique_ptr<UCP::Endpoint>, Create, (UCP::Worker & worker, char * server_addr, uint16_t
      port), (override));
};

TEST(ConnectionManager, Empty) {
  UCP::Context context(false);
  UCP::Worker worker = UCP::Worker(context);
  MockEndpointFactory mock_endpoint_factory;
  EXPECT_CALL(mock_endpoint_factory, Create(::testing::Ref(worker), (char *) "localhost", 1234)).Times(1);
  ConnectionManager connection_manager = ConnectionManager(worker, &mock_endpoint_factory);
  connection_manager.GetConnection("localhost", 1234);
}