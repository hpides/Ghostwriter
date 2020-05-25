
#include <rembrandt/network/ucx/context.h>
#include <rembrandt/consumer/consumer_config.h>
#include <unordered_set>
#include <tbb/concurrent_queue.h>
#include <boost/program_options.hpp>
#include <rembrandt/protocol/message_generator.h>
#include <rembrandt/network/ucx/endpoint_factory.h>
#include <rembrandt/network/request_processor.h>
#include <rembrandt/network/connection_manager.h>
#include <rembrandt/consumer/receiver.h>
#include <rembrandt/consumer/direct_consumer.h>
#include <rembrandt/logging/throughput_logger.h>
#include <rembrandt/benchmark/rate_limiter.h>
#include <rembrandt/network/attached_message.h>
#include <iostream>
//#include <openssl/md5.h>

namespace po = boost::program_options;

int main(int argc, char *argv[]) {
  UCP::Context context(true);
  ConsumerConfig config;
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
  config.receive_buffer_size = config.max_batch_size * 3;

  const size_t kNumBuffers = 10;
  std::unordered_set<std::unique_ptr<char>> pointers;
  tbb::concurrent_bounded_queue<char *> free_buffers;
  tbb::concurrent_bounded_queue<char *> received_buffers;
  for (size_t _ = 0; _ < kNumBuffers; _++) {
    std::unique_ptr<char> pointer((char *) malloc(config.max_batch_size));
    free_buffers.push(pointer.get());
    pointers.insert(std::move(pointer));
  }
  UCP::Impl::Worker worker(context);
  MessageGenerator message_generator;
  UCP::EndpointFactory endpoint_factory;
  RequestProcessor request_processor(worker);
  ConnectionManager connection_manager(worker, &endpoint_factory, message_generator, request_processor);
  Receiver receiver(connection_manager, message_generator, request_processor, worker, config);
  DirectConsumer consumer(receiver, config);
  std::atomic<long> counter = 0;
  std::string filename = "rembrandt_consumer_log_" + std::to_string(config.max_batch_size);
  ThroughputLogger logger = ThroughputLogger(counter, log_directory, filename, config.max_batch_size);
  TopicPartition topic_partition(1, 1);

  const size_t batch_count = 20; //1000l * 1000 * 1000 * 100 / config.max_batch_size;
//  data_generator.Run(batch_count);
  logger.Start();
  auto start = std::chrono::high_resolution_clock::now();
  char *buffer;
  for (long count = 0; count < batch_count; count++) {
//    if (count % 100000 == 0) {
//      printf("Iteration: %d\n", count);
//    }
    bool freed = free_buffers.try_pop(buffer);
    if (!freed) {
      throw std::runtime_error("Could not receive free buffer. Queue was empty.");
    }
    consumer.Receive(topic_partition, std::make_unique<AttachedMessage>(buffer, config.max_batch_size));
    ++counter;
//    std::unique_ptr<unsigned char[]> md5 = std::make_unique<unsigned char[]>(MD5_DIGEST_LENGTH);
//    unsigned char *ret = MD5((const unsigned char *) buffer, config.max_batch_size, md5.get());
//    std::clog << "MD5 #" << std::dec << count << ": ";
//    for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
//      std::clog << std::hex << ((int) md5[i]);
//    }
//    std::clog << "\n";
    free_buffers.push(buffer);
  }

  auto stop = std::chrono::high_resolution_clock::now();
  logger.Stop();

  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";

}
