#include <iostream>
#include <boost/program_options.hpp>
#include <rembrandt/benchmark/ysb/kafka/producer.h>
#include <rembrandt/logging/throughput_logger.h>

// class ThroughputLoggingDeliveryReportCb : public RdKafka::DeliveryReportCb {
//  public:
//   explicit ThroughputLoggingDeliveryReportCb(std::atomic<long> &counter,
//                                           tbb::concurrent_bounded_queue<char *> &free_buffers) :
//       counter_(counter),
//       free_buffers_(free_buffers) {}
//   void dr_cb(RdKafka::Message &message) override {
//     if (message.err()) {
//       exit(1);
//     } else {
//       ++counter_;
//       free_buffers_.push((char *) message.msg_opaque());
//     }
//   }
//  private:
//   std::atomic<long> &counter_;
//   tbb::concurrent_bounded_queue<char *> &free_buffers_;
// };

// class LatencyLoggingDeliveryReportCb : public RdKafka::DeliveryReportCb {
//  public:
//   explicit LatencyLoggingDeliveryReportCb(std::atomic<long> &counter,
//                                           tbb::concurrent_bounded_queue<char *> &free_buffers,
//                                           LatencyLogger &event_latency_logger,
//                                           LatencyLogger &processing_latency_logger) :
//       counter_(counter),
//       free_buffers_(free_buffers),
//       event_latency_logger_(event_latency_logger),
//       processing_latency_logger_(processing_latency_logger) {}
//   void dr_cb(RdKafka::Message &message) override {
//     if (message.err()) {
//       exit(1);
//     } else {
//       auto now = std::chrono::steady_clock::now();
//       long after = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
//       long before_event = *(long *) message.msg_opaque();
//       long before_processing = *(((long *) message.msg_opaque()) + 1);
//       event_latency_logger_.Log(after - before_event);
//       processing_latency_logger_.Log(after - before_processing);
//       ++counter_;
//       free_buffers_.push((char *) message.msg_opaque());
//     }
//   }
//  private:
//   std::atomic<long> &counter_;
//   tbb::concurrent_bounded_queue<char *> &free_buffers_;
//   LatencyLogger &event_latency_logger_;
//   LatencyLogger &processing_latency_logger_;
// };

YSBKafkaProducer::YSBKafkaProducer(int argc, char *const *argv)
    : kconfig_p_(std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL))) {
  this->ParseOptions(argc, argv);
  this->ConfigureKafka();

  std::string errstr;
  producer_p_ = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(kconfig_p_.get(), errstr));
  if (!producer_p_) {
    std::cerr << "Failed to create producer: " << errstr << std::endl;
    exit(1);
  }

  ReadIntoMemory();
}

void YSBKafkaProducer::ReadIntoMemory() {
  FILE *f = fopen(input_path_.c_str(), "rb");
  fseek(f, 0, SEEK_END);
  fsize_ = ftell(f);
  // TODO: Assert that fsize >= data_size
  fseek(f, 0, SEEK_SET);  /* same as rewind(f); */

  input_p_ = (char *) malloc(fsize_ + 1);
  fread(input_p_, 1, fsize_, f);
  fclose(f);

  input_p_[fsize_] = 0;
}

void YSBKafkaProducer::Run() {
  std::cout << "Starting logger..." << std::endl;
  std::atomic<long> counter = 0;
  ThroughputLogger logger =
      ThroughputLogger(counter, config_.log_directory, "benchmark_producer_throughput", config_.max_batch_size);
  logger.Start();
  std::cout << "Preparing run..." << std::endl;

  auto start = std::chrono::high_resolution_clock::now();

  char *buffer;

  std::cout << "Starting run execution..." << std::endl;
  long numBatchesInFile = fsize_ / GetBatchSize();
  for (size_t count = 0; count < GetRunBatchCount(); count++) {
    if (count % (GetRunBatchCount() / 10) == 0) {
      std::cout << "Iteration: " << count << std::endl;
    }
    void *message_p_ = input_p_ + GetBatchSize() * (count % numBatchesInFile);
    producer_p_->produce(std::string("ysb"), RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_BLOCK, message_p_, GetBatchSize(), nullptr, 0, 0, nullptr, message_p_);
    producer_p_->poll(0);
    ++counter;
  }
  std::cout << "Finishing run execution..." << std::endl;
  auto stop = std::chrono::high_resolution_clock::now();
  logger.Stop();
  std::cout << "Finished logger." << std::endl;
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";
}

void YSBKafkaProducer::ParseOptions(int argc, char *const *argv) {
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
         "Size of an individual batch (sending unit) in bytes")
        ("data-size", po::value(&config_.data_size)->default_value(config_.data_size),
         "Total amount of data transferred in this benchmark")
        ("warmup-fraction", po::value(&config_.warmup_fraction)->default_value(config_.warmup_fraction),
         "Fraction of data that is transferred during warmup")
        ("rate-limit", po::value(&config_.rate_limit)->default_value(config_.rate_limit),
         "The maximum amount of data that is transferred per second")
        ("input",
         po::value(&input_path_)->default_value(
             "/hpi/fs00/home/hendrik.makait/ghostwriter/ysb1B0.bin"),
         "File to load generated YSB data")
        ("log-dir",
         po::value(&config_.log_directory)->default_value(
             "/hpi/fs00/home/hendrik.makait/rembrandt/logs/20200727/e2e/50/kafka/"),
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


void YSBKafkaProducer::ConfigureKafka() {
  std::string errstr;
  if (kconfig_p_->set("bootstrap.servers", config_.broker_node_ip, errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (kconfig_p_->set("batch.num.messages", "1`", errstr)
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

  if (kconfig_p_->set("acks", "-1", errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  // TODO
  // ThroughputLoggingDeliveryReportCb br_dr_cb(counter, free_buffers, event_latency_logger, processing_latency_logger);
  // if (conf->set("dr_cb", &br_dr_cb, errstr) != RdKafka::Conf::CONF_OK) {
  //   std::cerr << errstr << std::endl;
  //   exit(1);
  // }
}


size_t YSBKafkaProducer::GetBatchCount() {
  return config_.data_size / GetBatchSize();
}

size_t YSBKafkaProducer::GetRunBatchCount() {
  return GetBatchCount() - GetWarmupBatchCount();
}
size_t YSBKafkaProducer::GetWarmupBatchCount() {
  return GetBatchCount() * config_.warmup_fraction;
}

size_t YSBKafkaProducer::GetBatchSize() {
  return (config_.max_batch_size / 128) * 128;
}

int main(int argc, char *argv[]) {
  YSBKafkaProducer producer(argc, argv);
  producer.Run();
}