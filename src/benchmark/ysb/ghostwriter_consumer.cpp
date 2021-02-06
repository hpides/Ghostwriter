
#include <rembrandt/network/ucx/context.h>
#include <rembrandt/consumer/consumer_config.h>
#include <unordered_set>
#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_hash_map.h>
#include <boost/program_options.hpp>
#include <rembrandt/protocol/message_generator.h>
#include <rembrandt/network/ucx/endpoint_factory.h>
#include <rembrandt/network/request_processor.h>
#include <rembrandt/network/connection_manager.h>
#include <rembrandt/consumer/receiver.h>
#include <rembrandt/consumer/direct_consumer.h>
#include <rembrandt/logging/throughput_logger.h>
#include <rembrandt/benchmark/rate_limiter.h>
#include <rembrandt/broker/broker_node.h>
#include <rembrandt/network/attached_message.h>
#include <iostream>
#include <rembrandt/logging/latency_logger.h>
#include <openssl/md5.h>
#include <rembrandt/benchmark/parallel_data_processor.h>
//#include "YahooBenchmark/YSB.cpp"

void LogMD5(size_t batch_size, const char *buffer, size_t count);
void Warmup(Consumer &consumer,
            size_t batch_count,
            size_t batch_size,
            tbb::concurrent_bounded_queue<char *> &free_buffers);
namespace po = boost::program_options;

int main(int argc, char *argv[]) {
  ConsumerConfig config = ConsumerConfig();
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
         po::value(&log_directory)->default_value("/hpi/fs00/home/hendrik.makait/rembrandt/logs/20200727/e2e/50/exclusive_opt/"),
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
  config.receive_buffer_size = config.max_batch_size * 3;

  config.mode = Partition::Mode::EXCLUSIVE;

  uint64_t effective_message_size;

  switch (config.mode) {
    case Partition::Mode::EXCLUSIVE:
      effective_message_size = config.max_batch_size;
      break;
    case Partition::Mode::CONCURRENT:
      effective_message_size = BrokerNode::GetConcurrentMessageSize(config.max_batch_size);
      break;
  }


  const size_t batch_count = 1024l * 1024 * 1024 * 80 / config.max_batch_size;
  const size_t kNumBuffers = 24;
  std::unordered_set<std::unique_ptr<char>> pointers;
  tbb::concurrent_bounded_queue<char *> free_buffers;
  tbb::concurrent_bounded_queue<char *> received_buffers;
  tbb::concurrent_hash_map<uint64_t, uint64_t> counts;
  for (size_t _ = 0; _ < kNumBuffers; _++) {
    std::unique_ptr<char> pointer((char *) malloc(effective_message_size));
    free_buffers.push(pointer.get());
    pointers.insert(std::move(pointer));
  }
  UCP::Context context(true);
  UCP::Impl::Worker worker(context);
  MessageGenerator message_generator;
  UCP::EndpointFactory endpoint_factory;
  RequestProcessor request_processor(worker);
  ConnectionManager connection_manager(worker, &endpoint_factory, message_generator, request_processor);
  Receiver receiver(connection_manager, message_generator, request_processor, worker, config);
  DirectConsumer consumer(receiver, config);
  std::atomic<long> counter = 0;
  std::string fileprefix = "rembrandt_consumer_" + std::to_string(config.max_batch_size) + "_1950";
  LatencyLogger processing_latency_logger = LatencyLogger(batch_count);
  LatencyLogger e2e_latency_logger = LatencyLogger(batch_count);
  ThroughputLogger logger = ThroughputLogger(counter, log_directory, fileprefix + "_throughput", config.max_batch_size);
  char *buffer;

  ParallelDataProcessor parallel_data_processor
      (config.max_batch_size, free_buffers, received_buffers, counts, 5);

  Warmup(consumer, batch_count, effective_message_size, free_buffers);

  parallel_data_processor.Start(batch_count);

  processing_latency_logger.Activate();
  e2e_latency_logger.Activate();
  logger.Start();
  auto start = std::chrono::high_resolution_clock::now();
  for (size_t count = 0; count < batch_count; count++) {
    if (count % (batch_count / 20) == 0) {
      printf("Iteration: %zu\n", count);
    }
    bool freed = free_buffers.try_pop(buffer);
    if (!freed) {
      throw std::runtime_error("Could not receive free buffer. Queue was empty.");
    }
//    free_buffers.pop(buffer);
    auto before = std::chrono::steady_clock::now();
    consumer.Receive(1, 1, std::make_unique<AttachedMessage>(buffer, effective_message_size));
    auto after = std::chrono::steady_clock::now();
    long e2e_before = *(long *) buffer;
    e2e_latency_logger.Log(
        std::chrono::duration_cast<std::chrono::microseconds>(after.time_since_epoch()).count() - e2e_before);
    processing_latency_logger.Log(std::chrono::duration_cast<std::chrono::microseconds>(after - before).count());

//    LogMD5(config.max_batch_size, buffer, count);

    ++counter;
    received_buffers.push(buffer);
  }

//  for (tbb::concurrent_hash_map<uint64_t, uint64_t>::iterator it = counts.begin(); it != counts.end(); it++) {
//    std::clog << it->first << ": " << it->second << std::endl;
//  }
  auto stop = std::chrono::high_resolution_clock::now();
  processing_latency_logger.Output(log_directory, fileprefix + "_processing");
  e2e_latency_logger.Output(log_directory, fileprefix + "_e2e");
  logger.Stop();
  parallel_data_processor.Stop();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";
}

void Warmup(Consumer &consumer,
            size_t batch_count,
            size_t batch_size,
            tbb::concurrent_bounded_queue<char *> &free_buffers) {
  size_t warmup_batch_count = batch_count / 10;
  char *buffer;
  for (size_t count = 0; count < warmup_batch_count; count++) {
    if (count % (warmup_batch_count / 2) == 0) {
      printf("Iteration: %zu\n", count);
    }
    bool freed = free_buffers.try_pop(buffer);
    if (!freed) {
      throw std::runtime_error("Could not receive free buffer. Queue was empty.");
    }
    consumer.Receive(1, 1, std::make_unique<AttachedMessage>(buffer, batch_size));
    free_buffers.push(buffer);
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
