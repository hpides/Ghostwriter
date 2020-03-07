#include <rembrandt/network/ucx/context.h>
#include <rembrandt/producer/producer_config.h>
#include <rembrandt/producer/producer.h>
#include <chrono>
#include <iostream>
#include <fcntl.h>
#include <rembrandt/network/request_processor.h>

int main(int argc, char *argv[]) {
  UCP::Context context(true);
  ProducerConfig config = ProducerConfig();

  config.storage_node_ip = (char *) "192.168.5.30";
  config.send_buffer_size = 1000 * 1000 * 1; // 10 MB
  config.max_batch_size = 1000 * 100; // 1 MB
  config.segment_size = 1000l * 1000 * 1000 * 10; // 10 GB

  UCP::Worker worker(context);
  UCP::EndpointFactory endpoint_factory;
  ConnectionManager connection_manager(worker, &endpoint_factory);
  MessageAccumulator message_accumulator(config.send_buffer_size, config.max_batch_size);
  RequestProcessor request_processor(worker);
  Sender sender(connection_manager, message_accumulator, request_processor, config);
  Producer producer(message_accumulator, sender, config);
  producer.Start();
  TopicPartition topic_partition(1, 1);
  void *random_buffer = malloc(config.max_batch_size);
//  void *buffer = malloc(config.max_batch_size);
  int fd = open("/dev/urandom", O_RDONLY);
  read(fd, random_buffer, config.max_batch_size);
  auto start = std::chrono::high_resolution_clock::now();
  for (uint32_t i = 0; i < 1000 * 1000; i++) {
    if (i % 100000 == 0) {
      printf("Iteration: %d\n", i);
    }
//        uint64_t offset = lrand48() % (config.max_batch_size * 9 + 1);
    producer.Send(topic_partition, ((char *) random_buffer), config.max_batch_size);
  }
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";
  producer.Stop();
}
