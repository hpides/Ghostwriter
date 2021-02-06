#ifndef REMBRANDT_SRC_CONSUMER_CONSUMER_CONFIG_H_
#define REMBRANDT_SRC_CONSUMER_CONSUMER_CONFIG_H_

#include <cstddef>
#include <string>
#include <rembrandt/broker/partition.h>

class ConsumerConfig {
 public:
  ConsumerConfig() = default;
  std::string storage_node_ip = "10.10.0.1";
  std::string broker_node_ip = "10.10.0.1";
  uint16_t broker_node_port = 13360;
  uint16_t storage_node_port = 13350;
  size_t receive_buffer_size = 16;
  size_t max_batch_size = 16;
  Partition::Mode mode = Partition::Mode::CONCURRENT;
  std::string log_directory = "/var/log/ghostwriter";
};

#endif //REMBRANDT_SRC_CONSUMER_CONSUMER_CONFIG_H_
