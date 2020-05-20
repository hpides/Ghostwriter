#include "rembrandt/network/connection_manager.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "rembrandt/network/ucx/worker.h"
#include "rembrandt/network/ucx/endpoint_factory.h"

namespace {
class MockEndpoint : public UCP::Endpoint {
 public:
  MockEndpoint() = default;
  ~MockEndpoint() {};
  MOCK_METHOD(void, RegisterRMemInfo, (const std::string &remote_key, uint64_t remote_addr), (override));
  MOCK_METHOD(ucp_rkey_h, GetRKey, (), (const, override));
  MOCK_METHOD(bool, hasRKey, (), (const, override));
  MOCK_METHOD(ucp_ep_h, GetHandle, (), (const, override));
  MOCK_METHOD(uint64_t, GetRemoteAddress, (), (const, override));
  MOCK_METHOD(ucs_status_ptr_t, receive, (void * buffer, size_t length, size_t * received_length), (const, override));
  MOCK_METHOD(ucs_status_ptr_t, send, (const void *buffer, size_t length), (const, override));
  MOCK_METHOD(ucs_status_ptr_t,
              put,
              (const void *buffer, size_t length, uint64_t remote_addr, ucp_send_callback_t cb),
              (const, override));
  MOCK_METHOD(ucs_status_ptr_t,
              get,
              (void * buffer, size_t length, uint64_t remote_addr, ucp_send_callback_t cb),
              (const, override));
  MOCK_METHOD(ucs_status_ptr_t,
              CompareAndSwap,
              (uint64_t compare, void * swap, size_t op_size, uint64_t remote_addr, ucp_send_callback_t cb),
              (const, override));
};

class MockWorker : public UCP::Worker {
 public:
  MockWorker() = default;
  ~MockWorker() override {};
  MOCK_METHOD(ucp_worker_h, GetWorkerHandle, (), (override));
  MOCK_METHOD(unsigned int, Progress, (), (override));
  MOCK_METHOD(ucs_status_t, Wait, (), (override));
};

class MockRequestProcessor : public RequestProcessor {
 public:
  explicit MockRequestProcessor(MockWorker &worker) : RequestProcessor(worker) {};
  ~MockRequestProcessor() override = default;
  MOCK_METHOD(ucs_status_t, Process, (void * status_ptr), (override));
};

class MockEndpointFactory : public UCP::EndpointFactory {
 public:
  MOCK_METHOD(std::unique_ptr<UCP::Endpoint>, Create, (UCP::Worker & worker, const std::string &server_addr, uint16_t
      port), (const, override));
};

class ConnectionManagerTest : public testing::Test {
 public:
  ConnectionManagerTest()
      : worker_(),
        message_generator_(),
        mock_endpoint_factory_(),
        mock_request_processor_(worker_),
        connection_manager_(worker_, &mock_endpoint_factory_, message_generator_, mock_request_processor_) {};
 protected:
  MockWorker worker_;
  MessageGenerator message_generator_;
  MockEndpointFactory mock_endpoint_factory_;
  MockRequestProcessor mock_request_processor_;
  ConnectionManager connection_manager_;
};

TEST_F(ConnectionManagerTest, GetNewConnection) {
  EXPECT_CALL(mock_endpoint_factory_, Create(::testing::Ref(worker_), (char *) "localhost", 1234)).Times(1);
  connection_manager_.GetConnection("localhost", 1234);
}

TEST_F(ConnectionManagerTest, GetKnownConnection) {
  // TODO: Fix second call by returning mocked pointer instead of nullptr
  ON_CALL(mock_endpoint_factory_, Create(::testing::Ref(worker_), (char *) "localhost", 1234))
      .WillByDefault(::testing::Return(::testing::ByMove(std::make_unique<MockEndpoint>())));
  EXPECT_CALL(mock_endpoint_factory_, Create(::testing::Ref(worker_), (char *) "localhost", 1234)).Times(1);
  connection_manager_.GetConnection("localhost", 1234);
  connection_manager_.GetConnection("localhost", 1234);
}
}