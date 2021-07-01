#include <iostream>
#include <boost/program_options.hpp>
#include <rembrandt/benchmark/producer.h>
#include <rembrandt/broker/broker_node.h>
#include <rembrandt/logging/throughput_logger.h>
#include <rembrandt/network/attached_message.h>

BenchmarkProducer::BenchmarkProducer(int argc, char *const *argv)
    : context_p_(std::make_unique<UCP::Context>(true)),
      free_buffers_p_(std::make_unique<tbb::concurrent_bounded_queue<char *>>()),
      generated_buffers_p_(std::make_unique<tbb::concurrent_bounded_queue<char *>>()) {
  const size_t kNumBuffers = 24;

  this->ParseOptions(argc, argv);

  uint64_t effective_message_size;

  producer_p_ = DirectProducer::Create(config_, *context_p_);

  for (size_t _ = 0; _ < kNumBuffers; _++) {
    std::unique_ptr<char> pointer((char *) malloc(effective_message_size));
    free_buffers_p_->push(pointer.get());
    buffers_p_->insert(std::move(pointer));
  }

  warmup_generator_p_ = ParallelDataGenerator::Create(config_.max_batch_size,
                                                      *free_buffers_p_,
                                                      *generated_buffers_p_,
                                                      config_.rate_limit,
                                                      0,
                                                      1000,
                                                      5,
                                                      MODE::STRICT);

  generator_p_ = ParallelDataGenerator::Create(config_.max_batch_size,
                                               *free_buffers_p_,
                                               *generated_buffers_p_,
                                               config_.rate_limit,
                                               0,
                                               1000,
                                               5,
                                               MODE::STRICT);  // TODO: Adjust mode init
}

void BenchmarkProducer::Warmup() {
  char *buffer;
  warmup_generator_p_->Start(GetWarmupBatchCount());

  for (size_t count = 0; count < GetWarmupBatchCount(); count++) {
    if (count % (GetWarmupBatchCount() / 10) == 0) {
      printf("Iteration: %zu\n", count);
    }
    bool generated = generated_buffers_p_->try_pop(buffer);
    if (!generated) {
      throw std::runtime_error("Could not receive generated buffer. Queue was empty.");
    }
    producer_p_->Send(1, 1, std::make_unique<AttachedMessage>(buffer, GetEffectiveBatchSize()));
    free_buffers_p_->push(buffer);
  }

  warmup_generator_p_->Stop();
}

void BenchmarkProducer::Run() {
  Warmup();
  std::atomic<long> counter = 0;
  ThroughputLogger logger =
      ThroughputLogger(counter, config_.log_directory, "benchmark_producer_throughput", config_.max_batch_size);
  generator_p_->Start(GetRunBatchCount());
  logger.Start();

  auto start = std::chrono::high_resolution_clock::now();

  char *buffer;
  for (size_t count = 0; count < GetRunBatchCount(); count++) {
    if (count % (GetRunBatchCount() / 10) == 0) {
      printf("Iteration: %zu\n", count);
    }
    generated_buffers_p_->pop(buffer);
    producer_p_->Send(1, 1, std::make_unique<AttachedMessage>(buffer, GetEffectiveBatchSize()));
    ++counter;
    free_buffers_p_->push(buffer);
  }
  auto stop = std::chrono::high_resolution_clock::now();
  logger.Stop();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";
  generator_p_->Stop();
}

void BenchmarkProducer::ParseOptions(int argc, char *const *argv) {
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
         "Size of an individual batch (sending unit) in bytes")
        ("data-size", po::value(&config_.data_size)->default_value(config_.data_size),
         "Total amount of data transferred in this benchmark")
        ("warmup-fraction", po::value(&config_.warmup_fraction)->default_value(config_.warmup_fraction),
         "Fraction of data that is transferred during warmup")
        ("rate-limit", po::value(&config_.rate_limit)->default_value(config_.rate_limit),
         "The maximum amount of data that is transferred per second")
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
     std::cout << "Could not parse mode: '" << mode_str <<"'" << std::endl;
     exit(1);
    }
  } catch (const po::error &ex) {
    std::cout << ex.what() << std::endl;
    exit(1);
  }
}

size_t BenchmarkProducer::GetBatchCount() {
  return config_.data_size / config_.max_batch_size;
}

size_t BenchmarkProducer::GetRunBatchCount() {
  return GetBatchCount() - GetWarmupBatchCount();
}
size_t BenchmarkProducer::GetWarmupBatchCount() {
  return GetBatchCount() * config_.warmup_fraction;
}

size_t BenchmarkProducer::GetEffectiveBatchSize() {
  switch (config_.mode) {
    case Partition::Mode::EXCLUSIVE:return config_.max_batch_size;
    case Partition::Mode::CONCURRENT:return BrokerNode::GetConcurrentMessageSize(config_.max_batch_size);
  }
}

int main(int argc, char *argv[]) {
  BenchmarkProducer producer(argc, argv);
  producer.Run();
}