#include <boost/program_options.hpp>
#include <chrono>
#include <iostream>
#include <fcntl.h>
#include <rembrandt/logging/throughput_logger.h>
#include <atomic>
#include <rembrandt/benchmark/data_generator.h>
#include <unordered_set>
#include <rdkafkacpp.h>
#include <rembrandt/benchmark/parallel_data_generator.h>
#include <tbb/concurrent_queue.h>

#include <hdr_histogram.h>
#include <rembrandt/logging/latency_logger.h>
namespace po = boost::program_options;

class BufferReturnDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  explicit BufferReturnDeliveryReportCb(std::atomic<long> &counter,
                                        tbb::concurrent_bounded_queue<char *> &free_buffers,
                                        LatencyLogger &latency_logger) :
      counter_(counter),
      free_buffers_(free_buffers),
      latency_logger_(latency_logger) {}
  void dr_cb(RdKafka::Message &message) override {
    if (message.err()) {
      exit(1);
    } else {
      auto now = std::chrono::steady_clock::now();
      long after = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
      long before = *(long *) message.msg_opaque();
      latency_logger_.Log(after - before);
      ++counter_;
      free_buffers_.push((char *) message.msg_opaque());
    }
  }
 private:
  std::atomic<long> &counter_;
  tbb::concurrent_bounded_queue<char *> &free_buffers_;
  LatencyLogger &latency_logger_;
};

void busy_polling(RdKafka::Producer *producer, std::atomic<bool> &running) {
  running = true;
  while (running) {
    producer->poll(-1);
  }
}

int main(int argc, char *argv[]) {
  size_t max_batch_size;
  std::string log_directory;
  try {
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("max-batch-size",
         po::value(&max_batch_size)->default_value(1024 * 128),
         "Maximum size of an individual batch (sending unit) in bytes")
        ("log-dir",
         po::value(&log_directory)->default_value("/home/hendrik.makait/rembrandt/logs/20200526/sustainable_throughput/"),
         "Directory to store throughput logs");

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

  const long RATE_LIMIT = 250l * 1000 * 1000;
  const size_t kNumBuffers = RATE_LIMIT / max_batch_size * 3;
  const size_t batch_count = 1024l * 1024 * 1024 * 10 / max_batch_size;
  char *buffer;
  std::unordered_set<std::unique_ptr<char>> pointers;
  tbb::concurrent_bounded_queue<char *> free_buffers;
  tbb::concurrent_bounded_queue<char *> generated_buffers;
  for (size_t _ = 0; _ < kNumBuffers; _++) {
    std::unique_ptr<char> pointer((char *) malloc(max_batch_size));
    free_buffers.push(pointer.get());
    pointers.insert(std::move(pointer));
  }
  std::atomic<long> counter = 0;

  std::string fileprefix = "kafka_producer_" + std::to_string(max_batch_size) + "_" + std::to_string(RATE_LIMIT);
  LatencyLogger latency_logger = LatencyLogger(batch_count, 100);
  ThroughputLogger logger = ThroughputLogger(counter, log_directory, fileprefix + "_throughput", max_batch_size);

  std::string topic = "TestTopic";

  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  std::string errstr;
  if (conf->set("bootstrap.servers", "10.10.0.11", errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (conf->set("batch.num.messages", "1`", errstr)
      != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (conf->set("message.max.bytes", "10000000", errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (conf->set("message.copy.max.bytes", "0", errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (conf->set("acks", "-1", errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  BufferReturnDeliveryReportCb br_dr_cb(counter, free_buffers, latency_logger);
  if (conf->set("dr_cb", &br_dr_cb, errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
  if (!producer) {
    std::cerr << "Failed to create producer: " << errstr << std::endl;
    exit(1);
  }

  delete conf;
  RateLimiter warmup_rate_limiter = RateLimiter::Create(RATE_LIMIT);
  ParallelDataGenerator
      warmup_data_generator(max_batch_size, free_buffers, generated_buffers, warmup_rate_limiter, 0, 1000, 1, MODE::RELAXED);


  std::atomic<bool> running = false;
  std::thread thread(busy_polling, producer, std::ref(running));

  warmup_data_generator.Start(batch_count);
  for (long count = 0; count < batch_count; count++) {
    if (count % (batch_count / 20) == 0) {
      printf("Warmup Iteration: %d\n", count);
    }
    generated_buffers.pop(buffer);
    producer->produce(topic,
                      RdKafka::Topic::PARTITION_UA,
                      RdKafka::Producer::RK_MSG_BLOCK,
                      buffer,
                      max_batch_size,
                      0,
                      0,
                      0,
                      nullptr,
                      buffer);
    producer->poll(0);
  }
  warmup_data_generator.Stop();

  while (counter < batch_count) {
    usleep(10);
  }
  counter = 0;
  RateLimiter rate_limiter = RateLimiter::Create(RATE_LIMIT);
  ParallelDataGenerator
      parallel_data_generator(max_batch_size, free_buffers, generated_buffers, rate_limiter, 0, 1000, 1, MODE::STRICT);
  parallel_data_generator.Start(batch_count);
  logger.Start();
  latency_logger.Activate();
  auto start = std::chrono::high_resolution_clock::now();

  for (long count = 0; count < batch_count; count++) {
    if (count % (batch_count / 20) == 0) {
      printf("Iteration: %d\n", count);
    }
    generated_buffers.pop(buffer);
    producer->produce(topic,
                      RdKafka::Topic::PARTITION_UA,
                      RdKafka::Producer::RK_MSG_BLOCK,
                      buffer,
                      max_batch_size,
                      0,
                      0,
                      0,
                      nullptr,
                      buffer);
    producer->poll(0);
  }

  while (counter < batch_count) {
    usleep(10);
  }
  auto stop = std::chrono::high_resolution_clock::now();
  latency_logger.Output(log_directory, fileprefix);
  logger.Stop();
  parallel_data_generator.Stop();
  running = false;
  thread.join();

  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";
}
