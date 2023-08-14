#include <memory>
#include <cstring>
#include <iostream>
#include <boost/program_options.hpp>
#include <rembrandt/network/ucx/context.h>
#include "rembrandt/network/server.h"

class TestServer : public MessageHandler {
    public:
        TestServer();
        void Run();
        std::unique_ptr<Message> HandleMessage(const Message &raw_message) override;
    private:
        std::unique_ptr<UCP::Context> context_p_;
        std::unique_ptr<Server> server_p_;
};

TestServer::TestServer(): MessageHandler(std::make_unique<MessageGenerator>()),
    context_p_(std::make_unique<UCP::Context>(false)) {

    std::unique_ptr<UCP::Worker> data_worker_p = context_p_->CreateWorker();
    std::unique_ptr<UCP::Worker> listening_worker_p = context_p_->CreateWorker();
    server_p_ = std::make_unique<Server>(std::move(data_worker_p), std::move(listening_worker_p), 13350);
}

std::unique_ptr<Message> TestServer::HandleMessage(const Message &raw_message) {
    auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(raw_message.GetBuffer());
    auto union_type = base_message->content_type();
    throw std::runtime_error(std::to_string(union_type)); 
}

void TestServer::Run() {
    server_p_->Run(this);
}

int main(int argc, char *argv[]) {
    // std::unique_ptr<MessageGenerator> message_generator_p = std::make_unique<MessageGenerator>();
    TestServer server_;
    server_.Run();
}
