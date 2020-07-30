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
#include <openssl/md5.h>
namespace po = boost::program_options;

void LogMD5(size_t batch_size, const char *buffer, size_t count);

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
         po::value(&log_directory)->default_value("/hpi/fs00/home/hendrik.makait/rembrandt/logs/20200727/processing_latency/kafka/"),
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

  const long RATE_LIMIT = 240l * 1000 * 1000;
  const size_t kNumBuffers = RATE_LIMIT / max_batch_size * 3;
  const size_t batch_count = 1000l * 1000 * 1000 * 80 / max_batch_size;
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
  LatencyLogger latency_logger = LatencyLogger(batch_count);
  ThroughputLogger logger = ThroughputLogger(counter, log_directory, fileprefix + "_throughput", max_batch_size);

  std::string topic = "TestTopic";

  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  std::string errstr;
  if (conf->set("bootstrap.servers", "10.150.1.12", errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (conf->set("batch.num.messages", "1`", errstr)
      != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (conf->set("message.max.bytes", std::to_string(max_batch_size * 1.1), errstr) != RdKafka::Conf::CONF_OK) {
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

  LatencyLoggingDeliveryReportCb br_dr_cb(counter, free_buffers, latency_logger);
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

  const size_t warmup_batch_count = batch_count / 10;

  warmup_data_generator.Start(warmup_batch_count);
  for (size_t count = 0; count < warmup_batch_count; count++) {
    if (count % (batch_count / 20) == 0) {
      printf("Warmup Iteration: %zu\n", count);
    }
    generated_buffers.pop(buffer);
    producer->produce(topic,
                      0,
                      RdKafka::Producer::RK_MSG_BLOCK,
                      buffer,
                      max_batch_size,
                      nullptr,
                      0,
                      0,
                      nullptr,
                      buffer);
    producer->poll(0);
  }
  warmup_data_generator.Stop();

  while ((size_t) counter < warmup_batch_count) {
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

  for (size_t count = 0; count < batch_count; count++) {
    if (count % (batch_count / 20) == 0) {
      printf("Iteration: %zu\n", count);
    }
    generated_buffers.pop(buffer);
//    LogMD5(max_batch_size, buffer, count);
    producer->produce(topic,
                      RdKafka::Topic::PARTITION_UA,
                      RdKafka::Producer::RK_MSG_BLOCK,
                      buffer,
                      max_batch_size,
                      nullptr,
                      0,
                      0,
                      nullptr,
                      buffer);
    producer->poll(0);
  }

  while ((size_t) counter < batch_count) {
    usleep(10);
  }
  auto stop = std::chrono::high_resolution_clock::now();
  latency_logger.Output(log_directory, fileprefix);
  logger.Stop();
  parallel_data_generator.Stop();
  std::cout << "Finished\n";
  running = false;
  thread.join();

  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";
}

void LogMD5(size_t batch_size, const char *buffer, size_t count) {
  std::unique_ptr<unsigned char[]> md5 = std::make_unique<unsigned char[]>(MD5_DIGEST_LENGTH);
  MD5((const unsigned char *) buffer, batch_size, md5.get());
  std::clog << "MD5 #" << std::dec << count << ": ";
  for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
    std::clog << std::hex << ((int) md5[i]);
  }
  std::clog << "\n";
}
