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

#include <stdio.h>
#include <stdlib.h>
#include <openssl/md5.h>
#include <rembrandt/benchmark/rate_limiter.h>

namespace po = boost::program_options;

void LogMD5(size_t batch_size, const char *buffer, size_t count);


void ReadIntoMemory(std::string input_file, char **buffer_p, long *fsize_p) {
  FILE *f = fopen(input_file.c_str(), "rb");
  fseek(f, 0, SEEK_END);
  long fsize = ftell(f);
  *fsize_p = fsize;
  fseek(f, 0, SEEK_SET);  /* same as rewind(f); */

  char *buffer= (char *) malloc(fsize + 1);
  fread(buffer, 1, fsize, f);
  fclose(f);

  buffer[fsize] = 0;
  *buffer_p = buffer;
}

int main(int argc, char *argv[]) {
  ProducerConfig config = ProducerConfig();
  std::string input_file;
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
        ("input-dir",
         po::value(&input_file)->default_value(
             "/hpi/fs00/home/hendrik.makait/ghostwriter-experiments/data/10m/ysb0.bin"),
         "File to load generated YSB data")
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

  long fsize;
  char *buffer;

  ReadIntoMemory(input_file, &buffer, &fsize);

//  const long RATE_LIMIT = 3900l * 1000 * 1000 * 0.50;
  size_t batch_size = (config.max_batch_size / 128) * 128;
  const size_t batch_count = fsize / batch_size;

  UCP::Context context(true);
  UCP::Impl::Worker worker(context);
  MessageGenerator message_generator = MessageGenerator();
  UCP::EndpointFactory endpoint_factory;
  RequestProcessor request_processor(worker);
  ConnectionManager connection_manager(worker, &endpoint_factory, message_generator, request_processor);
  Sender sender(connection_manager, message_generator, request_processor, worker, config);
  DirectProducer producer(sender, config);
  TopicPartition topic_partition(1, 1);
  std::atomic<long> counter = 0;

  const int NUM_SEGMENTS = 90;

  std::string fileprefix =
      "rembrandt_producer_" + std::to_string(config.max_batch_size) + "_" + std::to_string(NUM_SEGMENTS);

  ThroughputLogger logger = ThroughputLogger(counter, log_directory, fileprefix + "_throughput", config.max_batch_size);
//  RateLimiter rate_limiter = RateLimiter::Create(RATE_LIMIT);

  logger.Start();
  for (size_t count = 0; count < batch_count; count++) {
    if (count % (batch_count / 20) == 0) {
      printf("Iteration: %zu\n", count);
    }
    producer.Send(topic_partition, std::make_unique<AttachedMessage>(buffer + (batch_size * count), batch_size));
    ++counter;
  }

  logger.Stop();

  std::cout << "Finished\n";
}
