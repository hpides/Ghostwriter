#ifndef REMBRANDT_SRC_PRODUCER_PRODUCER_CONFIG_H_
#define REMBRANDT_SRC_PRODUCER_PRODUCER_CONFIG_H_

class ProducerConfig {
 public:
  ProducerConfig() = default;
  char *storage_node_ip = (char *) "192.168.5.31";
  uint16_t storage_node_port = 13350;
  uint16_t storage_node_rkey_port = 13351;
  uint64_t segment_size = 1024;
  size_t send_buffer_size = 16;
  size_t max_batch_size = 16;
};

#endif //REMBRANDT_SRC_PRODUCER_PRODUCER_CONFIG_H_
