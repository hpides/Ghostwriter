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

int main(int argc, char *argv[]) {
  UCP::Context context(true);
  ProducerConfig config = ProducerConfig();

  config.storage_node_ip = (char *) "10.10.0.11";
  config.broker_node_ip = (char *) "10.10.0.11";
  config.broker_node_port = 13360;
  config.send_buffer_size = 131072 * 3;
  config.max_batch_size = 131072;

  UCP::Worker worker(context);
  UCP::EndpointFactory endpoint_factory;
  RequestProcessor request_processor(worker);
  ConnectionManager connection_manager(worker, &endpoint_factory);
  MessageGenerator message_generator = MessageGenerator();
  Sender sender(connection_manager, message_generator, request_processor, worker, config);
  DirectProducer producer(sender, config);
  std::atomic<long> counter = 0;
  ThroughputLogger logger = ThroughputLogger(counter, ".", config.max_batch_size);
  TopicPartition topic_partition(1, 1);

  void *random_buffer = malloc(config.max_batch_size);
  void *buffer = malloc(config.max_batch_size);
  int fd = open("/dev/urandom", O_RDONLY);
  read(fd, random_buffer, config.max_batch_size);

  logger.Start();
  auto start = std::chrono::high_resolution_clock::now();

  for (counter; counter < 1000l * 1000 * 1000 * 100 / config.max_batch_size; counter++) {
    if (counter % 100000 == 0) {
      printf("Iteration: %d\n", counter.load());
    }
//        uint64_t offset = lrand48() % (config.max_batch_size * 9 + 1);
    producer.Send(topic_partition, ((char *) random_buffer), config.max_batch_size);
  }

  auto stop = std::chrono::high_resolution_clock::now();
  logger.Stop();

  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";
}
