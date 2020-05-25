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
//#include <openssl/md5.h>
#define NUM_KEYS 1000

namespace po = boost::program_options;

int main(int argc, char *argv[]) {
  ProducerConfig config = ProducerConfig();
  std::string log_directory;
  try {
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("broker-node-ip",
         po::value(&config.broker_node_ip)->default_value("10.10.0.11"),
         "IP address of the broker node")
        ("broker-node-port",
         po::value(&config.broker_node_port)->default_value(13360),
         "Port number of the broker node")
        ("storage-node-ip",
         po::value(&config.storage_node_ip)->default_value("10.10.0.11"),
         "IP address of the storage node")
        ("storage-node-port",
         po::value(&config.storage_node_port)->default_value(13350),
         "Port number of the storage node")
        ("max-batch-size",
         po::value(&config.max_batch_size)->default_value(131072),
         "Maximum size of an individual batch (sending unit) in bytes")
        ("log-dir",
         po::value(&log_directory)->default_value("/home/hendrik.makait/rembrandt/logs/"),
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
//  long throughput_per_second = 3l * 1000 * 1000 * 1000;
  config.send_buffer_size = config.max_batch_size * 3;
  const size_t kNumBuffers = 30; //throughput_per_second / config.max_batch_size * 3;
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
  std::atomic<long> counter = 0;
  std::string filename = "rembrandt_producer_log_" + std::to_string(config.max_batch_size);
  ThroughputLogger logger = ThroughputLogger(counter, log_directory, filename, config.max_batch_size);
  struct hdr_histogram* histogram;
  hdr_init(1, INT64_C(3600000000), 3, &histogram);
  TopicPartition topic_partition(1, 1);
  RateLimiter rate_limiter = RateLimiter::Create(1l * 1000 * 1000 * 1000);
  ParallelDataGenerator parallel_data_generator
      (config.max_batch_size, free_buffers, generated_buffers, rate_limiter, 0, 1000, 1, MODE::RELAXED);
//  DataGenerator data_generator(config.max_batch_size, free_buffers, generated_buffers, rate_limiter, 0, 1000, MODE::RELAXED);
  const size_t batch_count = 1024l * 1024 * 1024 * 10 / config.max_batch_size;
  parallel_data_generator.Start(batch_count);
  logger.Start();
  auto start = std::chrono::high_resolution_clock::now();
  char *buffer;
  for (long count = 0; count < batch_count; count++) {
//    if (count % (batch_count / 20) == 0) {
//      printf("Iteration: %d\n", count);
//    }
    generated_buffers.pop(buffer);
//    std::unique_ptr<unsigned char[]> md5 = std::make_unique<unsigned char[]>(MD5_DIGEST_LENGTH);
//    unsigned char *ret = MD5((const unsigned char *) buffer, config.max_batch_size, md5.get());
//    std::clog << "MD5 #" << std::dec << count << ": ";
//    for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
//      std::clog << std::hex << ((int) md5[i]);
//    }
//    std::clog << "\n";
    producer.Send(topic_partition, std::make_unique<AttachedMessage>(buffer, config.max_batch_size));
    auto now = std::chrono::steady_clock::now();
    long after = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    long before = *(long *) buffer;
    if (count > (batch_count / 100 * 5)) {
      hdr_record_value(histogram, after - before);
    }
    ++counter;
    free_buffers.push(buffer);
  }
  hdr_percentiles_print(histogram, stdout, 5, 1.0, CLASSIC);
  fflush(stdout);
  auto stop = std::chrono::high_resolution_clock::now();
  logger.Stop();
  parallel_data_generator.Stop();

  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";
}
