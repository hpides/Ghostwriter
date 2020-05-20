#ifndef REMBRANDT_SRC_NETWORK_REQUEST_PROCESSOR_H_
#define REMBRANDT_SRC_NETWORK_REQUEST_PROCESSOR_H_

#include <rembrandt/network/ucx/worker.h>

class RequestProcessor {
 public:
  explicit RequestProcessor(UCP::Worker &worker);
  virtual ~RequestProcessor() = default;
  virtual ucs_status_t Process(void *status_ptr);
 private:
  UCP::Worker &worker_;
};

#endif //REMBRANDT_SRC_NETWORK_REQUEST_PROCESSOR_H_
