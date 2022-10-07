#include <iostream>
#include <boost/program_options.hpp>
#include <rembrandt/benchmark/ysb/kafka/consumer.h>
#include <rembrandt/logging/throughput_logger.h>

YSBKafkaConsumer::YSBKafkaConsumer(int argc, char *const *argv)
    : kconfig_p_(std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL))), 
      kconfig_topic_p_(std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC))),
      received_messages_p_(std::make_unique<tbb::concurrent_bounded_queue<RdKafka::Message *>>()) {
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

  ysb_p_ = std::make_unique<KafkaYSB>(GetBatchSize(), *received_messages_p_);
}

void YSBKafkaConsumer::Run() {
  std::cout << "Starting logger..." << std::endl;
  std::atomic<long> counter = 0;
  ThroughputLogger logger =
      ThroughputLogger(counter, config_.log_directory, "benchmark_consumer_throughput", GetBatchSize());
  logger.Start();

  std::cout << "Preparing run..." << std::endl;

 SystemConf::getInstance().BUNDLE_SIZE = GetBatchSize();
 SystemConf::getInstance().BATCH_SIZE = GetBatchSize();
 SystemConf::getInstance().CIRCULAR_BUFFER_SIZE = 8388608;
  std::thread data_processor_thread(&KafkaYSB::runBenchmark, *ysb_p_, true);

  auto start = std::chrono::high_resolution_clock::now();

  char *buffer;

  RdKafka::Message *msg;
  std::cout << "Starting run execution..." << std::endl;
  for (size_t count = 0; count < GetBatchCount(); count++) {
    if (count % (GetBatchCount() / 10) == 0) {
      std::cout <<"Iteration: " << count << std::endl;
    }
    do {
      msg = consumer_p_->consume(topic_p_.get(), 0, 1000);
    } while (msg->err() != RdKafka::ERR_NO_ERROR);
    ++counter;
    received_messages_p_->push(msg);
  }
  std::cout << "Finishing run execution..." << std::endl;
  data_processor_thread.join();
  auto stop = std::chrono::high_resolution_clock::now();
  logger.Stop();
  std::cout << "Finished logger." << std::endl;
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";
}

void YSBKafkaConsumer::ParseOptions(int argc, char *const *argv) {
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
  } catch (const po::error &ex) {
    std::cout << ex.what() << std::endl;
    exit(1);
  }
}

void YSBKafkaConsumer::ConfigureKafka() {
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

size_t YSBKafkaConsumer::GetBatchCount() {
  return config_.data_size / GetBatchSize();
}

size_t YSBKafkaConsumer::GetBatchSize() {
  return (config_.max_batch_size / 128) * 128;
}

int main(int argc, char *argv[]) {
  YSBKafkaConsumer consumer(argc, argv);
  consumer.Run();
}