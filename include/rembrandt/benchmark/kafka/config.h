#ifndef REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_KAFKA_CONFIG_H
#define REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_KAFKA_CONFIG_H

struct ProducerConfig {
  std::string broker_node_ip = "10.10.0.1";
  uint16_t broker_node_port = 13360;
  size_t send_buffer_size = 16;
  size_t max_batch_size;
  size_t data_size = 1024l * 1024 * 1024 * 80;
  float warmup_fraction = 0.1;
  size_t rate_limit = 1000l * 1000 * 1000 * 10;
  std::string log_directory = "/tmp/log/kafka";
};

struct ConsumerConfig {
  std::string broker_node_ip = "10.10.0.1";
  uint16_t broker_node_port = 9092;
  size_t max_batch_size;
  size_t data_size = 1024l * 1024 * 1024 * 80;
  float warmup_fraction = 0.1;
  std::string log_directory = "/tmp/log/kafka";
};

#endif //REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_KAFKA_CONFIG_H