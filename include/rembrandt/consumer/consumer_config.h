#ifndef REMBRANDT_SRC_CONSUMER_CONSUMER_CONFIG_H_
#define REMBRANDT_SRC_CONSUMER_CONSUMER_CONFIG_H_

#include <cstdint>
#include <cstdlib>

inline char *IPADDRESS = "192.168.5.31";

class ConsumerConfig {
 public:
  ConsumerConfig() = default;
  char *storage_node_ip = IPADDRESS;
  char *broker_node_ip = IPADDRESS;
  uint16_t broker_node_port = 13360;
  uint16_t storage_node_port = 13350;
  uint16_t storage_node_rkey_port = 13351;
  size_t receive_buffer_size = 16;
  size_t max_batch_size = 16;
};

#endif //REMBRANDT_SRC_CONSUMER_CONSUMER_CONFIG_H_
