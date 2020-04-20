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
#define NUM_KEYS 1000

int main(int argc, char *argv[]) {
  UCP::Context context(true);
  ProducerConfig config = ProducerConfig();

  config.storage_node_ip = (char *) "10.10.0.11";
  config.broker_node_ip = (char *) "10.10.0.12";
  config.broker_node_port = 13360;
  config.send_buffer_size = 131072 * 3;
  config.max_batch_size = 131072;

  const size_t kNumBuffers = 10;
  std::unordered_set<std::unique_ptr<char>> pointers;
  tbb::concurrent_bounded_queue<char *> free_buffers;
  tbb::concurrent_bounded_queue<char *> generated_buffers;
  for (size_t _ = 0; _ < kNumBuffers; _++) {
    std::unique_ptr<char> pointer((char *) malloc(config.max_batch_size));
    free_buffers.push(pointer.get());
    pointers.insert(std::move(pointer));
  }
  UCP::Impl::Worker worker(context);
  MessageGenerator message_generator = MessageGenerator();
  UCP::EndpointFactory endpoint_factory(message_generator);
  RequestProcessor request_processor(worker);
  ConnectionManager connection_manager(worker, &endpoint_factory);
  Sender sender(connection_manager, message_generator, request_processor, worker, config);
  DirectProducer producer(sender, config);
  std::atomic<long> counter = 0;
  ThroughputLogger logger = ThroughputLogger(counter, ".", config.max_batch_size);
  TopicPartition topic_partition(1, 1);
  RateLimiter rate_limiter = RateLimiter::Create(1000l * 1000 * 1000);
  DataGenerator data_generator(config.max_batch_size, free_buffers, generated_buffers, rate_limiter, 0, 1000);

  const size_t batch_count = 1000l * 1000 * 1000 * 100 / config.max_batch_size;
  data_generator.Run(batch_count);
  logger.Start();
  auto start = std::chrono::high_resolution_clock::now();
  char *buffer;
  for (long count = 0; count < batch_count; count++) {
    if (count % 100000 == 0) {
      printf("Iteration: %d\n", count);
    }
    generated_buffers.pop(buffer);
    producer.Send(topic_partition, std::make_unique<AttachedMessage>(buffer, config.max_batch_size));
    ++counter;
    free_buffers.push(buffer);
  }

  auto stop = std::chrono::high_resolution_clock::now();
  logger.Stop();

  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";
}
