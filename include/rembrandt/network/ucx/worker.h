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
  explicit Worker(Context &ucp_context);
  virtual ~Worker();
  Worker(const Worker &) = delete;
  Worker &operator=(const Worker &) = delete;
  ucp_worker_h GetWorkerHandle() { return worker_; };
  unsigned int Progress() { return ucp_worker_progress(worker_); };
  ucs_status_t Wait() { return ucp_worker_wait(worker_); };
  friend bool operator==(const Worker &lhs, const Worker &rhs) { return lhs.worker_ == rhs.worker_; };
 private:
  int fd;
  ucp_worker_h worker_;
};
}

#endif //REMBRANDT_SRC_NETWORK_UCX_WORKER_H_
