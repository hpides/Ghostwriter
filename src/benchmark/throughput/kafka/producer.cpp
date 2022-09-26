#include <iostream>
#include <boost/program_options.hpp>
#include <rdkafkacpp.h>
#include <rembrandt/benchmark/throughput/kafka/producer.h>
#include <rembrandt/logging/throughput_logger.h>
#include <rembrandt/logging/latency_logger.h> // needed?

class ThroughputLoggingDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  explicit ThroughputLoggingDeliveryReportCb(std::atomic<long> &counter,
                                          tbb::concurrent_bounded_queue<char *> &free_buffers) :
      counter_(counter),
      free_buffers_(free_buffers) {}
  void dr_cb(RdKafka::Message &message) override {
    if (message.err()) {
      exit(1);
    } else {
      ++counter_;
      free_buffers_.push((char *) message.msg_opaque());
    }
  }
 private:
  std::atomic<long> &counter_;
  tbb::concurrent_bounded_queue<char *> &free_buffers_;
};

class LatencyLoggingDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  explicit LatencyLoggingDeliveryReportCb(std::atomic<long> &counter,
                                          tbb::concurrent_bounded_queue<char *> &free_buffers,
                                          LatencyLogger &event_latency_logger,
                                          LatencyLogger &processing_latency_logger) :
      counter_(counter),
      free_buffers_(free_buffers),
      event_latency_logger_(event_latency_logger),
      processing_latency_logger_(processing_latency_logger) {}
  void dr_cb(RdKafka::Message &message) override {
    if (message.err()) {
      exit(1);
    } else {
      auto now = std::chrono::steady_clock::now();
      long after = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
      long before_event = *(long *) message.msg_opaque();
      long before_processing = *(((long *) message.msg_opaque()) + 1);
      event_latency_logger_.Log(after - before_event);
      processing_latency_logger_.Log(after - before_processing);
      ++counter_;
      free_buffers_.push((char *) message.msg_opaque());
    }
  }
 private:
  std::atomic<long> &counter_;
  tbb::concurrent_bounded_queue<char *> &free_buffers_;
  LatencyLogger &event_latency_logger_;
  LatencyLogger &processing_latency_logger_;
};

void busy_polling(RdKafka::Producer &producer, std::atomic<bool> &running) {
  running = true;
  while (running) {
    producer.poll(-1);
  }
}


BenchmarkProducer::BenchmarkProducer(int argc, char *const *argv)
    : kconfig_(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)), running_(false),
      free_buffers_p_(std::make_unique < tbb::concurrent_bounded_queue < char * >> ()),
      generated_buffers_p_(std::make_unique < tbb::concurrent_bounded_queue < char * >> ()) {
  const size_t kNumBuffers = 32;

  this->ParseOptions(argc, argv);
  this->ConfigureKafka();

  std::string errstr;
  producer_p_ = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(kconfig_, errstr));
  if (!producer_p_) {
    std::cerr << "Failed to create producer: " << errstr << std::endl;
    exit(1);
  }

  for (size_t _ = 0; _ < kNumBuffers; _++) {
    //TODO: Improved RAII
    auto pointer = (char *) malloc(config_.max_batch_size);
    free_buffers_p_->push(pointer);
  }

  warmup_generator_p_ = ParallelDataGenerator::Create(config_.max_batch_size,
                                                      config_.rate_limit,
                                                      0,
                                                      1000,
                                                      5,
                                                      MODE::RELAXED);

  generator_p_ = ParallelDataGenerator::Create(config_.max_batch_size,
                                               config_.rate_limit,
                                               0,
                                               1000,
                                               16,
                                               MODE::RELAXED);  // TODO: Adjust mode init

  polling_thread_p_ =  std::make_unique<std::thread>(busy_polling, std::ref(*producer_p_), std::ref(running_));
}

void BenchmarkProducer::Warmup() {
  std::cout << "Starting warmup ..." << std::endl;
  char *buffer;
  std::atomic<long> counter = 0l;
  warmup_generator_p_->Start(GetWarmupBatchCount(), *free_buffers_p_, *generated_buffers_p_);

  for (size_t count = 0; count < GetWarmupBatchCount(); count++) {
//    if (count % (GetWarmupBatchCount() / 10) == 0) {
      std::cout << "Iteration: " << count << std::endl;
//    }
    generated_buffers_p_->pop(buffer);
    producer_p_->produce(std::string("benchmark"), RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_BLOCK,
    buffer, config_.max_batch_size, nullptr, 0, 0, nullptr, buffer);
    producer_p_->poll(0);

  }
  warmup_generator_p_->Stop();
  while((size_t) counter < GetWarmupBatchCount()) {
    usleep(10);
  }
  std::cout << "Finished warmup!" << std::endl;
}

void BenchmarkProducer::Run() {
  Warmup();

  std::cout << "Starting logger..." << std::endl;
  std::atomic<long> counter = 0l;
  ThroughputLogger logger =
      ThroughputLogger(counter, config_.log_directory, "benchmark_producer_throughput", config_.max_batch_size);
  generator_p_->Start(GetRunBatchCount(), *free_buffers_p_, *generated_buffers_p_);
  logger.Start();
  std::cout << "Preparing run..." << std::endl;

  auto start = std::chrono::high_resolution_clock::now();

  char *buffer;

  std::cout << "Starting run execution..." << std::endl;
  while (GetRunBatchCount() > 0 && generated_buffers_p_->size() == 0) {}
  for (size_t count = 0; count < GetRunBatchCount(); count++) {
    if (count % (GetRunBatchCount() / 10) == 0) {
      std::cout << "Iteration: " << count << std::endl;
    }
    if (!generated_buffers_p_->try_pop(buffer)) {
      throw std::runtime_error("Could not pop from generated buffer, queue is empty.");
    }
    producer_p_->produce(std::string("benchmark"), RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_BLOCK, buffer, config_.max_batch_size, nullptr, 0, 0, nullptr, buffer);
  }
  while ((size_t) counter < GetRunBatchCount()) {
    usleep(10);
  }
  std::cout << "Finishing run execution..." << std::endl;
  auto stop = std::chrono::high_resolution_clock::now();
  logger.Stop();
  std::cout << "Finished logger." << std::endl;
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms" << std::endl;
  generator_p_->Stop();
  running_ = false;
  polling_thread_p_->join();
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

void BenchmarkProducer::ConfigureKafka() {
  std::string errstr;
  if (kconfig_->set("bootstrap.servers", config_.broker_node_ip, errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (kconfig_->set("batch.num.messages", "1`", errstr)
      != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (kconfig_->set("message.max.bytes", std::to_string(config_.max_batch_size * 1.1), errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (kconfig_->set("message.copy.max.bytes", "0", errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (kconfig_->set("acks", "-1", errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
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

int main(int argc, char *argv[]) {
  BenchmarkProducer producer(argc, argv);
  producer.Run();
}