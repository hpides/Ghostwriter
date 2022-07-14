#include <iostream>
#include <boost/program_options.hpp>
#include <rembrandt/benchmark/ysb/ghostwriter/consumer.h>
#include <rembrandt/logging/throughput_logger.h>
#include <rembrandt/network/attached_message.h>
#include <rembrandt/protocol/protocol.h>

YSBGhostwriterConsumer::YSBGhostwriterConsumer(int argc, char *const *argv)
    : context_p_(std::make_unique<UCP::Context>(true)),
      free_buffers_p_(std::make_unique<tbb::concurrent_bounded_queue<char *>>()),
      received_buffers_p_(std::make_unique<tbb::concurrent_bounded_queue<char *>>()) {
  const size_t kNumBuffers = 32;

  this->ParseOptions(argc, argv);

  consumer_p_ = DirectConsumer::Create(config_, *context_p_);

  for (size_t _ = 0; _ < kNumBuffers; _++) {
    auto pointer = (char *) malloc(GetEffectiveBatchSize());
    free_buffers_p_->push(pointer);
  }

  ysb_p_ = std::make_unique<GhostwriterYSB>(GetBatchSize(), *free_buffers_p_, *received_buffers_p_);
}

//void YSBGWConsumer::Warmup() {
//  char *buffer;
//  for (size_t count = 0; count < GetWarmupBatchCount(); count++) {
//    if (count % (GetWarmupBatchCount() / 10) == 0) {
//      std::cout <<"Warmup Iteration: " << count << std::endl;
//    }
//    bool freed = free_buffers_p_->try_pop(buffer);
//    if (!freed) {
//      throw std::runtime_error("Could not receive free buffer. Queue was empty.");
//    }
//    consumer_p_->Receive(1, 1, std::make_unique<AttachedMessage>(buffer, GetEffectiveBatchSize()));
//    free_buffers_p_->push(buffer);
//  }
//}

void YSBGhostwriterConsumer::Run() {
//  Warmup();
  std::cout << "Starting logger..." << std::endl;
  std::atomic<long> counter = 0;
  ThroughputLogger logger =
      ThroughputLogger(counter, config_.log_directory, "benchmark_consumer_throughput", GetBatchSize());
  logger.Start();

  std::cout << "Preparing run..." << std::endl;

 SystemConf::getInstance().BUNDLE_SIZE = GetBatchSize();
 SystemConf::getInstance().BATCH_SIZE = GetBatchSize();
 SystemConf::getInstance().CIRCULAR_BUFFER_SIZE = 8388608;
  std::thread data_processor_thread(&GhostwriterYSB::runBenchmark, *ysb_p_, true);

  auto start = std::chrono::high_resolution_clock::now();

  char *buffer;

  std::cout << "Starting run execution..." << std::endl;
  for (size_t count = 0; count < GetBatchCount(); count++) {
    if (count % (GetBatchCount() / 10) == 0) {
      std::cout <<"Iteration: " << count << std::endl;
    }
    free_buffers_p_->pop(buffer);
    consumer_p_->Receive(1, 1, std::make_unique<AttachedMessage>(buffer, GetEffectiveBatchSize()));

    ++counter;
    received_buffers_p_->push(buffer);
  }
  std::cout << "Finishing run execution..." << std::endl;
  data_processor_thread.join();
  auto stop = std::chrono::high_resolution_clock::now();
  logger.Stop();
  std::cout << "Finished logger." << std::endl;
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";
}

void YSBGhostwriterConsumer::ParseOptions(int argc, char *const *argv) {
  namespace po = boost::program_options;
  std::string mode_str;
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
        ("batch-size",
         po::value(&config_.max_batch_size)->default_value(131072),
         "Maximum size of an individual batch (sending unit) in bytes")
        ("data-size", po::value(&config_.data_size)->default_value(config_.data_size),
         "Total amount of data transferred in this benchmark")
        ("warmup-fraction", po::value(&config_.warmup_fraction)->default_value(config_.warmup_fraction),
         "Fraction of data that is transferred during warmup")
        ("log-dir",
         po::value(&config_.log_directory)->default_value(
             "/hpi/fs00/home/hendrik.makait/rembrandt/logs/20200727/e2e/50/exclusive_opt/"),
         "Directory to store benchmark logs")
        ("mode", po::value(&mode_str), "The mode in which the producer is run, 'exclusive' or 'concurrent'");

    po::variables_map variables_map;
    po::store(po::parse_command_line(argc, argv, desc), variables_map);
    po::notify(variables_map);

    if (variables_map.count("help")) {
      std::cout << "Usage: myExecutable [options]\n";
      std::cout << desc;
      exit(0);
    }
    if (mode_str == "exclusive") {
      config_.mode = Partition::Mode::EXCLUSIVE;
    } else if (mode_str == "concurrent") {
      config_.mode = Partition::Mode::CONCURRENT;
    } else {
      std::cout << "Could not parse mode: '" << mode_str << "'" << std::endl;
      exit(1);
    }
  } catch (const po::error &ex) {
    std::cout << ex.what() << std::endl;
    exit(1);
  }
}

size_t YSBGhostwriterConsumer::GetBatchCount() {
  return config_.data_size / GetBatchSize();
}

size_t YSBGhostwriterConsumer::GetBatchSize() {
  return (config_.max_batch_size / 128) * 128;
}

size_t YSBGhostwriterConsumer::GetEffectiveBatchSize() {
  return Protocol::GetEffectiveBatchSize(GetBatchSize(), config_.mode);
}

int main(int argc, char *argv[]) {
  YSBGhostwriterConsumer consumer(argc, argv);
  consumer.Run();
}