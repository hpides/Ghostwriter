#ifndef REMBRANDT_SRC_PRODUCER_PRODUCER_CONFIG_H_
#define REMBRANDT_SRC_PRODUCER_PRODUCER_CONFIG_H_

#include <cstddef>
#include <string>
#include <rembrandt/broker/partition.h>

struct ProducerConfig {
  std::string storage_node_ip = "10.10.0.1";
  std::string broker_node_ip = "10.10.0.1";
  uint16_t broker_node_port = 13360;
  uint16_t storage_node_port = 13350;
  size_t send_buffer_size = 16;
  size_t max_batch_size = 16;
  size_t data_size = 1024l * 1024 * 1024 * 80;
  float warmup_fraction = 0.1;
  Partition::Mode mode = Partition::Mode::CONCURRENT;
  size_t rate_limit = 1000l * 1000 * 1000 * 10;
  std::string log_directory = "/tmp/log/ghostwriter";
};

#endif // REMBRANDT_SRC_PRODUCER_PRODUCER_CONFIG_H_
