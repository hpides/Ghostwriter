#include <rembrandt/network/ucx/context.h>
#include <rembrandt/producer/producer_config.h>
#include <rembrandt/producer/producer.h>
#include <chrono>
#include <iostream>
#include <fcntl.h>
#include <rembrandt/network/request_processor.h>
#include <rembrandt/logging/throughput_logger.h>

int main(int argc, char *argv[]) {
  UCP::Context context(false);
  ProducerConfig config = ProducerConfig();

  config.storage_node_ip = (char *) "192.168.5.30";
  config.broker_node_ip = (char *) "192.168.5.30";
  config.broker_node_port = 13360;
  config.send_buffer_size = 1000 * 1000 * 1; // 10 MB
  config.max_batch_size = 100; // 1 MB

  UCP::Worker worker(context);
  UCP::EndpointFactory endpoint_factory;
  RequestProcessor request_processor(worker);
  ConnectionManager connection_manager(worker, &endpoint_factory);
  MessageAccumulator message_accumulator(config.send_buffer_size, config.max_batch_size);
  MessageGenerator message_generator = MessageGenerator();
  Sender sender(connection_manager, message_accumulator, message_generator, request_processor, worker, config);
  Producer producer(message_accumulator, sender, config);
  atomic_long counter = 0;
  ThroughputLogger logger = ThroughputLogger(counter, "", config.max_batch_size);
  producer.Start();
  TopicPartition topic_partition(1, 1);
  void *random_buffer = malloc(config.max_batch_size);
//  void *buffer = malloc(config.max_batch_size);
  int fd = open("/dev/urandom", O_RDONLY);
  read(fd, random_buffer, config.max_batch_size);
  logger.Start();
  auto start = std::chrono::high_resolution_clock::now();
  for (counter; counter < 1000 * 1000; counter++) {
    if (counter % 100000 == 0) {
      printf("Iteration: %d\n", counter);
    }
//        uint64_t offset = lrand48() % (config.max_batch_size * 9 + 1);
    producer.Send(topic_partition, ((char *) random_buffer), config.max_batch_size);
  }
  auto stop = std::chrono::high_resolution_clock::now();
  logger.Stop();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";
  producer.Stop();
}
