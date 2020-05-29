#include <rembrandt/storage/storage_node.h>
#include "gtest/gtest.h"

namespace {
class StorageNodeTest : public testing::Test {
 protected:
  void SetUp() override {
    context_ = std::make_unique<UCP::Context>(true);
    StorageNodeConfig config;
    config.region_size = 1l * 1000 * 1000;
    config.server_port = 13350;
    std::unique_ptr<UCP::Impl::Worker> data_worker = std::make_unique<UCP::Impl::Worker>(*context_);
    std::unique_ptr<UCP::Impl::Worker> listening_worker = std::make_unique<UCP::Impl::Worker>(*context_);
    std::unique_ptr<Server>
        server = std::make_unique<Server>(std::move(data_worker), std::move(listening_worker), config.server_port);
    std::unique_ptr<StorageRegion> storage_region = std::make_unique<StorageRegion>(256, 256);
    std::unique_ptr<UCP::MemoryRegion> memory_region = std::make_unique<UCP::MemoryRegion>(*context_, *storage_region);
    std::unique_ptr<StorageManager>
        storage_manager = std::make_unique<StorageManager>(std::move(storage_region), config);
    std::unique_ptr<MessageGenerator> message_generator = std::make_unique<MessageGenerator>();
    storage_node_ = std::make_unique<StorageNode>(std::move(server),
                                                  std::move(memory_region),
                                                  std::move(message_generator),
                                                  std::move(storage_manager),
                                                  config);
    auto runner = [](StorageNode &storage_node) { storage_node.Run(); };
    node_thread_ = std::thread(runner, std::ref(*storage_node_));
  }

  void TearDown() override {
    storage_node_->Stop();
    if (node_thread_.joinable()) {
      node_thread_.join();
    }
  }
  std::unique_ptr<UCP::Context> context_;
  std::unique_ptr<StorageNode> storage_node_;
  std::thread node_thread_;
};

TEST_F(StorageNodeTest, Simple) {
  std::this_thread::sleep_for(std::chrono::seconds(1));
}
}
