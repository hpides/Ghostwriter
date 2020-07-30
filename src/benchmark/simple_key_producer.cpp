#include <boost/program_options.hpp>
#include <rembrandt/network/ucx/context.h>
#include <rembrandt/producer/producer_config.h>
#include <rembrandt/producer/direct_producer.h>
#include <chrono>
#include <iostream>
#include <fcntl.h>
#include <rembrandt/network/request_processor.h>
#include <rembrandt/logging/throughput_logger.h>
#include <atomic>
#include <rembrandt/network/connection_manager.h>
#include <rembrandt/protocol/message_generator.h>
#include <rembrandt/network/attached_message.h>
#include <rembrandt/benchmark/data_generator.h>
#include <unordered_set>
#include <rembrandt/benchmark/parallel_data_generator.h>

#include <hdr_histogram.h>
#include <rembrandt/logging/latency_logger.h>
#include <openssl/md5.h>
#define NUM_KEYS 1000

namespace po = boost::program_options;

void LogMD5(size_t batch_size, const char *buffer, size_t count);
void warmup(const long RATE_LIMIT,
            const size_t batch_count,
            DirectProducer &producer,
            const TopicPartition &topic_partition,
            ProducerConfig &config,
            tbb::concurrent_bounded_queue<char *> &free_buffers,
            tbb::concurrent_bounded_queue<char *> &generated_buffers) {
  RateLimiter warmup_rate_limiter = RateLimiter::Create(RATE_LIMIT);
  ParallelDataGenerator warmup_data_generator
      (config.max_batch_size, free_buffers, generated_buffers, warmup_rate_limiter, 0, 1000, 1, RELAXED);
  warmup_data_generator.Start(batch_count / 10);
  char *buffer;
  for (size_t count = 0; count < batch_count / 10; count++) {
    if (count % (batch_count / 20) == 0) {
      printf("Warmup Iteration: %zu\n", count);
    }
    generated_buffers.pop(buffer);
    producer.Send(topic_partition, std::make_unique<AttachedMessage>(buffer, config.max_batch_size));
    free_buffers.push(buffer);
  }
  warmup_data_generator.Stop();
}
int main(int argc, char *argv[]) {
  ProducerConfig config = ProducerConfig();
  std::string log_directory;
  try {
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("broker-node-ip",
         po::value(&config.broker_node_ip)->default_value("10.150.1.12"),
         "IP address of the broker node")
        ("broker-node-port",
         po::value(&config.broker_node_port)->default_value(13360),
         "Port number of the broker node")
        ("storage-node-ip",
         po::value(&config.storage_node_ip)->default_value("10.150.1.12"),
         "IP address of the storage node")
        ("storage-node-port",
         po::value(&config.storage_node_port)->default_value(13350),
         "Port number of the storage node")
        ("max-batch-size",
         po::value(&config.max_batch_size)->default_value(131072),
         "Maximum size of an individual batch (sending unit) in bytes")
        ("log-dir",
         po::value(&log_directory)->default_value(
             "/hpi/fs00/home/hendrik.makait/rembrandt/logs/20200727/e2e/50/exclusive_opt/"),
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
  const long RATE_LIMIT = 3900l * 1000 * 1000 * 0.50;
  config.send_buffer_size = config.max_batch_size * 3;
  const size_t batch_count = 1024l * 1024 * 1024 * 80 / config.max_batch_size;
  const size_t kNumBuffers = RATE_LIMIT / config.max_batch_size;
  std::unordered_set<std::unique_ptr<char>> pointers;
  tbb::concurrent_bounded_queue<char *> free_buffers;
  tbb::concurrent_bounded_queue<char *> generated_buffers;
  for (size_t _ = 0; _ < kNumBuffers; _++) {
    std::unique_ptr<char> pointer((char *) malloc(config.max_batch_size));
    free_buffers.push(pointer.get());
    pointers.insert(std::move(pointer));
  }
  UCP::Context context(true);
  UCP::Impl::Worker worker(context);
  MessageGenerator message_generator = MessageGenerator();
  UCP::EndpointFactory endpoint_factory;
  RequestProcessor request_processor(worker);
  ConnectionManager connection_manager(worker, &endpoint_factory, message_generator, request_processor);
  Sender sender(connection_manager, message_generator, request_processor, worker, config);
  DirectProducer producer(sender, config);
  TopicPartition topic_partition(1, 1);
  char *buffer;
  std::atomic<long> counter = 0;

  const int NUM_SEGMENTS = 90;

  std::string fileprefix =
      "rembrandt_producer_" + std::to_string(config.max_batch_size) + "_" + std::to_string(NUM_SEGMENTS) + "_"
          + std::to_string(RATE_LIMIT);
  LatencyLogger event_latency_logger = LatencyLogger(batch_count);
  LatencyLogger processing_latency_logger = LatencyLogger(batch_count);
//  LatencyLogger waiting_latency_logger = LatencyLogger(batch_count);
//  LatencyLogger staging_latency_logger = LatencyLogger(batch_count);
//  LatencyLogger storing_latency_logger = LatencyLogger(batch_count);
//  LatencyLogger committing_latency_logger = LatencyLogger(batch_count);
  ThroughputLogger logger = ThroughputLogger(counter, log_directory, fileprefix + "_throughput", config.max_batch_size);
  RateLimiter rate_limiter = RateLimiter::Create(RATE_LIMIT);
  ParallelDataGenerator parallel_data_generator
      (config.max_batch_size, free_buffers, generated_buffers, rate_limiter, 0, 1000, 6, MODE::STRICT);

  warmup(
      RATE_LIMIT,
      batch_count,
      producer,
      topic_partition, config,
      free_buffers,
      generated_buffers);

  event_latency_logger.Activate();
  processing_latency_logger.Activate();
//  waiting_latency_logger.Activate();
//  staging_latency_logger.Activate();
//  storing_latency_logger.Activate();
//  committing_latency_logger.Activate();
  logger.Start();
  parallel_data_generator.Start(batch_count);
//  auto start = std::chrono::high_resolution_clock::now();
  for (size_t count = 0; count < batch_count; count++) {
    if (count % (batch_count / 20) == 0) {
      printf("Iteration: %zu\n", count);
    }
    generated_buffers.pop(buffer);
//    LogMD5(config.max_batch_size, buffer, count);

    auto proc_before = std::chrono::steady_clock::now();
//    uint64_t latencies[4];
//    producer.Send(topic_partition, std::make_unique<AttachedMessage>(buffer, config.max_batch_size), latencies);
    producer.Send(topic_partition, std::make_unique<AttachedMessage>(buffer, config.max_batch_size));
    auto after = std::chrono::steady_clock::now();
    long event_before = *(long *) buffer;
    event_latency_logger.Log(
        std::chrono::duration_cast<std::chrono::microseconds>(after.time_since_epoch()).count() - event_before);
//    waiting_latency_logger.Log(latencies[0]);
//    staging_latency_logger.Log(latencies[1]);
//    storing_latency_logger.Log(latencies[2]);
//    committing_latency_logger.Log(latencies[3]);
    processing_latency_logger.Log(std::chrono::duration_cast<std::chrono::microseconds>(after - proc_before).count());
    ++counter;
    free_buffers.push(buffer);
  }
//  auto stop = std::chrono::high_resolution_clock::now();
  event_latency_logger.Output(log_directory, fileprefix + "_event");
  processing_latency_logger.Output(log_directory, fileprefix + "_processing");
//  waiting_latency_logger.Output(log_directory, fileprefix + "_waiting");
//  staging_latency_logger.Output(log_directory, fileprefix + "_staging");
//  storing_latency_logger.Output(log_directory, fileprefix + "_storing");
//  committing_latency_logger.Output(log_directory, fileprefix + "_committing");
  logger.Stop();
  parallel_data_generator.Stop();

  std::cout << "Finished\n";
  while (true) {
    sleep(1);
  }
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
