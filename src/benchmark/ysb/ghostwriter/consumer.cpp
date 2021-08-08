#include <iostream>
#include <boost/program_options.hpp>
#include <rembrandt/benchmark/ysb/ghostwriter/consumer.h>
#include <rembrandt/broker/broker_node.h>
#include <rembrandt/logging/throughput_logger.h>
#include <rembrandt/network/attached_message.h>

YSBGWConsumer::YSBGWConsumer(int argc, char *const *argv)
    : context_p_(std::make_unique<UCP::Context>(true)),
      free_buffers_p_(std::make_unique<tbb::concurrent_bounded_queue<char *>>()),
      received_buffers_p_(std::make_unique<tbb::concurrent_bounded_queue<char *>>()) {
  const size_t kNumBuffers = 24;

  this->ParseOptions(argc, argv);

  config_.mode = Partition::Mode::EXCLUSIVE;

  uint64_t effective_message_size;

  consumer_p_ = DirectConsumer::Create(config_, *context_p_);

  for (size_t _ = 0; _ < kNumBuffers; _++) {
    std::unique_ptr<char> pointer((char *) malloc(effective_message_size));
    free_buffers_p_->push(pointer.get());
    buffers_p_->insert(std::move(pointer));
  }

  std::unique_ptr<GhostwriterYSB> ysb_p = std::make_unique<GhostwriterYSB>(config_.max_batch_size, *free_buffers_p_, *received_buffers_p_);
}

void YSBGWConsumer::Warmup() {
  char *buffer;
  for (size_t count = 0; count < GetWarmupBatchCount(); count++) {
    if (count % (GetWarmupBatchCount() / 10) == 0) {
      std::cout <<"Warmup Iteration: " << count << std::endl;
    }
    bool freed = free_buffers_p_->try_pop(buffer);
    if (!freed) {
      throw std::runtime_error("Could not receive free buffer. Queue was empty.");
    }
    consumer_p_->Receive(1, 1, std::make_unique<AttachedMessage>(buffer, GetEffectiveBatchSize()));
    free_buffers_p_->push(buffer);
  }
}

void YSBGWConsumer::Run() {
  Warmup();
  std::atomic<long> counter = 0;
  ThroughputLogger logger =
      ThroughputLogger(counter, config_.log_directory, "benchmark_consumer_throughput", config_.max_batch_size);
  std::thread data_processor_thread(&GhostwriterYSB::runBenchmark, *ysb_p_, true);
  logger.Start();

  auto start = std::chrono::high_resolution_clock::now();

  char *buffer;
  for (size_t count = 0; count < GetRunBatchCount(); count++) {
    if (count % (GetRunBatchCount() / 10) == 0) {
      std::cout <<"Iteration: " << count << std::endl;
    }
    free_buffers_p_->pop(buffer);
    consumer_p_->Receive(1, 1, std::make_unique<AttachedMessage>(buffer, GetEffectiveBatchSize()));

    ++counter;
    received_buffers_p_->push(buffer);
  }
  auto stop = std::chrono::high_resolution_clock::now();
  logger.Stop();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";
}

void YSBGWConsumer::ParseOptions(int argc, char *const *argv) {
  namespace po = boost::program_options;
  try {
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("broker-node-ip",
         po::value(&config_.broker_node_ip)->default_value("10.150.1.12"),
         "IP address of the broker node")
        ("broker-node-port",
         po::value(&config_.broker_node_port)->default_value(13360),
         "Port number of the broker node")
        ("storage-node-ip",
         po::value(&config_.storage_node_ip)->default_value("10.150.1.12"),
         "IP address of the storage node")
        ("storage-node-port",
         po::value(&config_.storage_node_port)->default_value(13350),
         "Port number of the storage node")
        ("max-batch-size",
         po::value(&config_.max_batch_size)->default_value(131072),
         "Maximum size of an individual batch (sending unit) in bytes")
        ("data-size", po::value(&config_.data_size)->default_value(config_.data_size),
         "Total amount of data transferred in this benchmark")
        ("warmup-fraction", po::value(&config_.warmup_fraction)->default_value(config_.warmup_fraction),
         "Fraction of data that is transferred during warmup")
        ("log-dir",
         po::value(&config_.log_directory)->default_value(
             "/hpi/fs00/home/hendrik.makait/rembrandt/logs/20200727/e2e/50/exclusive_opt/"),
         "Directory to store benchmark logs");

    po::variables_map variables_map;
    po::store(po::parse_command_line(argc, argv, desc), variables_map);
    po::notify(variables_map);

    if (variables_map.count("help")) {
      std::cout << "Usage: myExecutable [options]\n";
      std::cout << desc;
      exit(0);
    }
  } catch (const po::error &ex) {
    std::cout << ex.what() << std::endl;
    exit(1);
  }
}

size_t YSBGWConsumer::GetBatchCount() {
  return config_.data_size / config_.max_batch_size;
}

size_t YSBGWConsumer::GetRunBatchCount() {
  return GetBatchCount() - GetWarmupBatchCount();
}
size_t YSBGWConsumer::GetWarmupBatchCount() {
  return GetBatchCount() * config_.warmup_fraction;
}

size_t YSBGWConsumer::GetEffectiveBatchSize() {
  switch (config_.mode) {
    case Partition::Mode::EXCLUSIVE:return config_.max_batch_size;
    case Partition::Mode::CONCURRENT:return BrokerNode::GetConcurrentMessageSize(config_.max_batch_size);
  }
}

int main(int argc, char *argv[]) {
  YSBGWConsumer consumer(argc, argv);
  consumer.Run();
}