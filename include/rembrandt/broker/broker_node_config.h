#ifndef REMBRANDT_SRC_BROKER_BROKER_NODE_CONFIG_H_
#define REMBRANDT_SRC_BROKER_BROKER_NODE_CONFIG_H_

class BrokerNodeConfig {
 public:
  uint64_t server_port = 13360;

  std::string storage_node_ip = "10.10.0.12";
  uint16_t storage_node_port = 13350;
  uint16_t storage_node_rkey_port = 13351;
};

#endif //REMBRANDT_SRC_BROKER_BROKER_NODE_CONFIG_H_
