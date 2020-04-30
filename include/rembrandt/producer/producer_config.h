#ifndef REMBRANDT_SRC_PRODUCER_PRODUCER_CONFIG_H_
#define REMBRANDT_SRC_PRODUCER_PRODUCER_CONFIG_H_

#include <cstddef>
#include <string>

class ProducerConfig {
 public:
  ProducerConfig() = default;
  std::string storage_node_ip = "10.10.0.1";
  std::string broker_node_ip = "10.10.0.1";
  uint16_t broker_node_port = 13360;
  uint16_t storage_node_port = 13350;
  uint16_t storage_node_rkey_port = 13351;
  size_t send_buffer_size = 16;
  size_t max_batch_size = 16;
};

#endif //REMBRANDT_SRC_PRODUCER_PRODUCER_CONFIG_H_
