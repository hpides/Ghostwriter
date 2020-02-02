#ifndef REMBRANDT_SRC_NETWORK_UCX_WORKER_H_
#define REMBRANDT_SRC_NETWORK_UCX_WORKER_H_

extern "C" {
#include "ucp/api/ucp.h"
#include "ucs/type/status.h"
}

#include "context.h"

namespace UCP {
class Worker {
 public:
  Worker(Context &ucp_context);
  ~Worker();
  ucp_worker_h GetWorkerHandle() { return worker_; };
  ucs_status_t Progress() { return (ucs_status_t) ucp_worker_progress(worker_); };
 private:
  ucp_worker_h worker_;
};
}

#endif //REMBRANDT_SRC_NETWORK_UCX_WORKER_H_
