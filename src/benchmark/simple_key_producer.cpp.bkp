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

#define NUM_KEYS 1000

int main(int argc, char *argv[]) {
  UCP::Context context(true);
  ProducerConfig config = ProducerConfig();

  config.storage_node_ip = (char *) "10.10.0.11";
  config.broker_node_ip = (char *) "10.10.0.12";
  config.broker_node_port = 13360;
  config.send_buffer_size = 131072 * 3;
  config.max_batch_size = 131072;

  UCP::Worker worker(context);
  MessageGenerator message_generator = MessageGenerator();
  UCP::EndpointFactory endpoint_factory = UCP::EndpointFactory(message_generator);
  RequestProcessor request_processor(worker);
  ConnectionManager connection_manager(worker, &endpoint_factory);
  Sender sender(connection_manager, message_generator, request_processor, worker, config);
  DirectProducer producer(sender, config);
  std::atomic<long> counter = 0;
  ThroughputLogger logger = ThroughputLogger(counter, ".", config.max_batch_size);
  TopicPartition topic_partition(1, 1);

  assert(config.max_batch_size % sizeof(uint64_t) == 0);

  uint64_t batch_size = config.max_batch_size / sizeof(uint64_t);
  uint64_t *buffer = (uint64_t *) calloc(batch_size, sizeof(uint64_t));
  for (uint64_t i = 0; i < batch_size; i++) {
    *(buffer + i) = i % NUM_KEYS;
  }

  logger.Start();
  auto start = std::chrono::high_resolution_clock::now();
  for (long count = 0; count < 1000l * 1000 * 1000 * 100 / config.max_batch_size; count++) {
    if (count % 100000 == 0) {
      printf("Iteration: %d\n", count);
    }
    producer.Send(topic_partition, std::make_unique<AttachedMessage>((char *) buffer, config.max_batch_size));
    ++counter;
  }

  auto stop = std::chrono::high_resolution_clock::now();
  logger.Stop();

  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";
}
