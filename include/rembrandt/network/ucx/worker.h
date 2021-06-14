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
  virtual ~Worker() = 0;
  virtual ucp_worker_h GetWorkerHandle() = 0;
  virtual unsigned int Progress() = 0;
  virtual ucs_status_t Wait() = 0;
  virtual ucs_status_t Signal() = 0;
};

namespace Impl {
class Worker : public UCP::Worker {
 public:
  Worker() = delete;
  virtual ~Worker();
  Worker(const Worker &other) = delete;
  Worker(Worker &&other) noexcept = delete;
  Worker &operator=(const Worker &) = delete;
  Worker &operator=(Worker &&other) noexcept = delete;
  ucp_worker_h GetWorkerHandle() { return worker_; };
  unsigned int Progress() { return ucp_worker_progress(worker_); };
  ucs_status_t Wait() { return ucp_worker_wait(worker_); };
  ucs_status_t Signal() { return ucp_worker_signal(worker_); };
  friend bool operator==(const Worker &lhs, const Worker &rhs) { return lhs.worker_ == rhs.worker_; };
 private:
  friend class UCP::Context;
  explicit Worker(Context &ucp_context);
  ucp_worker_h worker_;
};
}
}

#endif //REMBRANDT_SRC_NETWORK_UCX_WORKER_H_
