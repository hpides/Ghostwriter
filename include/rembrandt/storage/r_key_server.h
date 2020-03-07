#ifndef REMBRANDT_SRC_STORAGE_R_KEY_SERVER_H_
#define REMBRANDT_SRC_STORAGE_R_KEY_SERVER_H_

#include <rembrandt/network/socket/static_server.h>
#include <rembrandt/network/ucx/memory_region.h>

class RKeyServer : public StaticServer {
 public:
  RKeyServer(UCP::MemoryRegion &memory_region);
};

#endif //REMBRANDT_SRC_STORAGE_R_KEY_SERVER_H_
