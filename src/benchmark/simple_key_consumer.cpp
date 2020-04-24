
#include <rembrandt/network/ucx/context.h>
#include <rembrandt/consumer/consumer_config.h>
#include <unordered_set>
#include <tbb/concurrent_queue.h>
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

int main(int argc, char *argv[]) {
  UCP::Context context(true);
  ConsumerConfig config;

  config.storage_node_ip = (char *) "10.10.0.11";
  config.broker_node_ip = (char *) "10.10.0.11";
  config.broker_node_port = 13360;
  config.receive_buffer_size = 131072 * 3;
  config.max_batch_size = 131072;

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
  UCP::EndpointFactory endpoint_factory(message_generator);
  RequestProcessor request_processor(worker);
  ConnectionManager connection_manager(worker, &endpoint_factory);
  Receiver receiver(connection_manager, message_generator, request_processor, worker, config);
  DirectConsumer consumer(receiver, config);
  std::atomic<long> counter = 0;
  ThroughputLogger logger(counter, ".", config.max_batch_size);
  TopicPartition topic_partition(1, 1);

  const size_t batch_count = 1000l * 1000 * 1000 * 100 / config.max_batch_size;
//  data_generator.Run(batch_count);
  logger.Start();
  auto start = std::chrono::high_resolution_clock::now();
  char *buffer;
  for (long count = 0; count < batch_count; count++) {
    if (count % 100000 == 0) {
      printf("Iteration: %d\n", count);
    }
    bool freed = free_buffers.try_pop(buffer);
    if (!freed) {
      throw std::runtime_error("Could not receive free buffer. Queue was empty.");
    }
    consumer.Receive(topic_partition, std::make_unique<AttachedMessage>(buffer, config.max_batch_size));
    ++counter;
    free_buffers.push(buffer);
  }

  auto stop = std::chrono::high_resolution_clock::now();
  logger.Stop();

  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";

}
