#include <rembrandt/storage/storage_node.h>
#include "gtest/gtest.h"

namespace {
class StorageNodeTest : public testing::Test {
 protected:
  void SetUp() override {
    UCP::Context context = UCP::Context(true);
    StorageNodeConfig config;
    config.region_size = 1l * 1000 * 1000;
    config.server_port = 13350;
    std::unique_ptr<UCP::Impl::Worker> data_worker = std::make_unique<UCP::Impl::Worker>(context);
    std::unique_ptr<UCP::Impl::Worker> listening_worker = std::make_unique<UCP::Impl::Worker>(context);
    std::unique_ptr<Server>
        server = std::make_unique<Server>(std::move(data_worker), std::move(listening_worker), config.server_port);
    std::unique_ptr<UCP::MemoryRegion> memory_region = std::make_unique<UCP::MemoryRegion>(context, config.region_size);
    std::unique_ptr<MessageGenerator> message_generator = std::make_unique<MessageGenerator>();
    std::unique_ptr<RKeyServer> r_key_server = std::make_unique<RKeyServer>(*memory_region);
    storage_node_ = std::make_unique<StorageNode>(std::move(server),
                                                  std::move(r_key_server),
                                                  std::move(memory_region),
                                                  std::move(message_generator),
                                                  config);
    auto runner = [](StorageNode &storage_node) { storage_node.Run(); };
    node_thread_ = std::thread(runner, std::ref(*storage_node_));
  }

  void TearDown() override {
    storage_node_->Stop();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    node_thread_.join();
  }
  std::unique_ptr<StorageNode> storage_node_;
  std::thread node_thread_;
};

TEST_F(StorageNodeTest, Simple) {
  std::this_thread::sleep_for(std::chrono::seconds(5));
}
}
