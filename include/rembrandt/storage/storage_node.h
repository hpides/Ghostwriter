#ifndef REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_
#define REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_

#include "rembrandt/network/server.h"
#include "rembrandt/storage/segment.h"

class StorageNode {
 public:
  StorageNode(UCP::Context &context,
              uint64_t region_size,
              uint64_t segment_size,
              uint32_t server_port,
              uint32_t rkey_port);
  void Run();
 private:
  Server server_;
  std::shared_ptr<Segment> segment_;
};

#endif //REMBRANDT_SRC_STORAGE_STORAGE_NODE_H_
