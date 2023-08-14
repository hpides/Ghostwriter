#include <iostream>
#include <rembrandt/network/ucx/context.h>
#include <rembrandt/network/ucx/worker.h>
#include <rembrandt/network/client.h>
#include <rembrandt/network/connection_manager.h>
#include <rembrandt/network/request_processor.h>
#include <rembrandt/protocol/message_generator.h>

class TestClient : public Client {
    public: 
        TestClient(std::unique_ptr<ConnectionManager> connection_manager_p,
            std::unique_ptr<MessageGenerator> message_generator_p,
            std::unique_ptr<RequestProcessor> request_processor_p,
            std::unique_ptr<UCP::Worker> worker_p);
        void Run();
        ~TestClient() = default;
    private:
      UCP::Endpoint &GetEndpointWithRKey() const override;
};

TestClient::TestClient(std::unique_ptr<ConnectionManager> connection_manager_p,
                       std::unique_ptr<MessageGenerator> message_generator_p,
                       std::unique_ptr<RequestProcessor> request_processor_p,
                       std::unique_ptr<UCP::Worker> worker_p)
    : Client(std::move(connection_manager_p), std::move(message_generator_p), std::move(request_processor_p), std::move(worker_p)) {}

void TestClient::Run() {
    UCP::Endpoint &endpoint = connection_manager_p_->GetConnection("172.20.26.67", 13350, false);
    std::unique_ptr<Message> message = message_generator_p_->InitializeRequest();
    SendMessage(*message, endpoint);
}


UCP::Endpoint &TestClient::GetEndpointWithRKey() const {
  return Client::GetEndpointWithRKey("172.20.26.67",
                                     13350);
}

int main(int argc, char *argv[]) {
    auto context_p = std::make_unique<UCP::Context>(false);
    auto message_generator_p = std::make_unique<MessageGenerator>();
    auto worker_p = context_p->CreateWorker();
    auto endpoint_factory_p = std::make_unique<UCP::EndpointFactory>();
    auto request_processor_p = std::make_unique<RequestProcessor>(*worker_p);
    auto connection_manager_p = std::make_unique<ConnectionManager>(std::move(endpoint_factory_p),
                                                                    *worker_p,
                                                                    *message_generator_p,
                                                                    *request_processor_p);
    auto client_ = TestClient(std::move(connection_manager_p),
                              std::move(message_generator_p),
                              std::move(request_processor_p),
                              std::move(worker_p));
    client_.Run();
}
