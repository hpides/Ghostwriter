#include <iostream>
#include <boost/program_options.hpp>
#include <rembrandt/benchmark/throughput/kafka/consumer.h>
#include <rembrandt/logging/throughput_logger.h>
#include <rembrandt/network/attached_message.h>
#include <rembrandt/protocol/protocol.h>

BenchmarkConsumer::BenchmarkConsumer(int argc, char *const *argv)
    : kconfig_p_(std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL))), 
      kconfig_topic_p_(std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC))),
      counts_p_(std::make_unique < tbb::concurrent_hash_map < uint64_t, uint64_t >> ()),
      received_messages_p_(std::make_unique < tbb::concurrent_bounded_queue < RdKafka::Message * >> ()) {
  const size_t kNumBuffers = 32;

  this->ParseOptions(argc, argv);
  this->ConfigureKafka();

  std::string errstr;
  consumer_p_ = std::unique_ptr<RdKafka::Consumer>(RdKafka::Consumer::create(kconfig_p_.get(), errstr));
  if (!consumer_p_) {
    std::cerr << "Failed to create consumer: " << errstr << std::endl;
    exit(1);
  }

  topic_p_ = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(consumer_p_.get(), "benchmark", kconfig_topic_p_.get(), errstr));
  if (!topic_p_) {
    std::cerr << "Failed to create topic: " << errstr << std::endl;
    exit(1);
  }

  warmup_processor_p_ = std::make_unique<KafkaParallelDataProcessor>(config_.max_batch_size,
                                                                *received_messages_p_,
                                                                *counts_p_,
                                                                2);

  processor_p_ = std::make_unique<KafkaParallelDataProcessor>(config_.max_batch_size,
                                                         *received_messages_p_,
                                                         *counts_p_,
                                                         2);
}

void BenchmarkConsumer::Warmup() {
  RdKafka::Message *msg;
  warmup_processor_p_->Start(GetWarmupBatchCount());

  for (size_t count = 0; count < GetWarmupBatchCount(); count++) {
    if (count % (GetWarmupBatchCount() / 10) == 0) {
      std::cout << "Iteration: " << count << std::endl;
    }
    do {
      msg = consumer_p_->consume(topic_p_.get(), 0, 1000);
    } while (msg->err() != RdKafka::ERR_NO_ERROR);
    received_messages_p_->push(msg);
  }
  warmup_processor_p_->Stop();
}

void BenchmarkConsumer::Run() {
  Warmup();
  std::cout << "Starting logger..." << std::endl;
  std::atomic<long> counter = 0;
  ThroughputLogger logger =
      ThroughputLogger(counter, config_.log_directory, "benchmark_consumer_throughput", config_.max_batch_size);
  processor_p_->Start(GetRunBatchCount());
  logger.Start();

  auto start = std::chrono::high_resolution_clock::now();

  RdKafka::Message *msg;

  for (size_t count = 0; count < GetRunBatchCount(); count++) {
    if (count % (GetRunBatchCount() / 10) == 0) {
      std::cout << "Iteration: " << count << std::endl;
    }
    do {
      msg = consumer_p_->consume(topic_p_.get(), 0, 1000);
    } while (msg->err() != RdKafka::ERR_NO_ERROR);
    ++counter;
    received_messages_p_->push(msg);
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
         po::value(&config_.broker_node_port)->default_value(9092),
         "Port number of the broker node")
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

void BenchmarkConsumer::ConfigureKafka() {
  std::string errstr;
  if (kconfig_p_->set("bootstrap.servers", config_.broker_node_ip, errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (kconfig_p_->set("enable.auto.commit", "false", errstr)
      != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (kconfig_p_->set("enable.auto.offset.store", "false", errstr)
      != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (kconfig_p_->set("message.max.bytes", std::to_string(config_.max_batch_size * 1.1), errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (kconfig_p_->set("message.copy.max.bytes", "0", errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }


  if (kconfig_p_->set("fetch.message.max.bytes", std::to_string(config_.max_batch_size * 1.1), errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (kconfig_p_->set("fetch.max.bytes", std::to_string(config_.max_batch_size * 1.1), errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
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

int main(int argc, char *argv[]) {
  BenchmarkConsumer consumer(argc, argv);
  consumer.Run();
}