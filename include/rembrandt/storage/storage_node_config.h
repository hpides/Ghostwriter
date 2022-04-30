#ifndef REMBRANDT_SRC_STORAGE_STORAGE_NODE_CONFIG_H_
#define REMBRANDT_SRC_STORAGE_STORAGE_NODE_CONFIG_H_

class StorageNodeConfig {
 public:
  enum class Type {PERSISTENT, VOLATILE};
  uint64_t region_size = 1024;
  uint64_t segment_size = 1024;
  uint32_t server_port = 13350;
  uint32_t message_size = 1024;
  Type type = Type::PERSISTENT;

};

#endif //REMBRANDT_SRC_STORAGE_STORAGE_NODE_CONFIG_H_
