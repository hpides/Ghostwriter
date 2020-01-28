#ifndef REMBRANDT_SRC_NETWORK_WORKER_H_
#define REMBRANDT_SRC_NETWORK_WORKER_H_

extern "C" {
#include <ucp/api/ucp.h>
#include <ucs/type/status.h>
}

#include "rembrandt/network/context.h"

class Worker {
 public:
  Worker(Context &ucp_context);
  ucp_worker_h ucp_worker_;
};

#endif //REMBRANDT_SRC_NETWORK_WORKER_H_
