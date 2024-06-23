#include <iostream>
#include <boost/program_options.hpp>
#include <rembrandt/benchmark/throughput/ghostwriter/consumer.h>
#include <rembrandt/logging/throughput_logger.h>
#include <rembrandt/network/attached_message.h>
#include <rembrandt/protocol/protocol.h>

BenchmarkConsumer::BenchmarkConsumer(int argc, char *const *argv)
    : context_p_(std::make_unique<UCP::Context>(true)),
      free_buffers_p_(std::make_unique < tbb::concurrent_bounded_queue < char * >> ()),
      received_buffers_p_(std::make_unique < tbb::concurrent_bounded_queue < char * >> ()),
      counts_p_(std::make_unique < tbb::concurrent_hash_map < uint64_t, uint64_t >> ()) {
  const size_t kNumBuffers = 32;

  this->ParseOptions(argc, argv);

  consumer_p_ = DirectConsumer::Create(config_, *context_p_);

  for (size_t _ = 0; _ < kNumBuffers; _++) {
    //TODO: Improved RAII
    auto pointer = (char *) malloc(GetEffectiveBatchSize());
    free_buffers_p_->push(pointer);
  }

  warmup_processor_p_ = std::make_unique<ParallelDataProcessor>(config_.max_batch_size,
                                                                *free_buffers_p_,
                                                                *received_buffers_p_,
                                                                *counts_p_,
                                                                5);

  processor_p_ = std::make_unique<ParallelDataProcessor>(config_.max_batch_size,
                                                         *free_buffers_p_,
                                                         *received_buffers_p_,
                                                         *counts_p_,
                                                         5);
}

void BenchmarkConsumer::Warmup() {
  char *buffer;
  warmup_processor_p_->Start(GetWarmupBatchCount());

  for (size_t count = 0; count < GetWarmupBatchCount(); count++) {
    if (count % (GetWarmupBatchCount() / 10) == 0) {
      std::cout << "Iteration: " << count << std::endl;
    }
    free_buffers_p_->pop(buffer);
    auto message = std::make_unique<AttachedMessage>(buffer, GetEffectiveBatchSize());
    consumer_p_->Receive(1, 1, std::move(message));
    received_buffers_p_->push(buffer);
  }
  warmup_processor_p_->Stop();
}

void BenchmarkConsumer::Run() {
  Warmup();
  std::cout << "Starting logger..." << std::endl;
  std::atomic<size_t> counter = 0;
  ThroughputLogger logger =
      ThroughputLogger(counter, config_.log_directory, "benchmark_consumer_throughput", config_.max_batch_size);
  processor_p_->Start(GetRunBatchCount());
  logger.Start();

  auto start = std::chrono::high_resolution_clock::now();

  char *buffer;

  for (size_t count = 0; count < GetRunBatchCount(); count++) {
    if (count % (GetRunBatchCount() / 10) == 0) {
      std::cout << "Iteration: " << count << std::endl;
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
  processor_p_->Stop();
}

void BenchmarkConsumer::ParseOptions(int argc, char *const *argv) {
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

size_t BenchmarkConsumer::GetBatchCount() {
  return config_.data_size / config_.max_batch_size;
}

size_t BenchmarkConsumer::GetRunBatchCount() {
  return GetBatchCount() - GetWarmupBatchCount();
}
size_t BenchmarkConsumer::GetWarmupBatchCount() {
  return GetBatchCount() * config_.warmup_fraction;
}

size_t BenchmarkConsumer::GetEffectiveBatchSize() {
  return Protocol::GetEffectiveBatchSize(config_.max_batch_size, config_.mode);
}

int main(int argc, char *argv[]) {
  BenchmarkConsumer consumer(argc, argv);
  consumer.Run();
}