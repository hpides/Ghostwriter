#ifndef REMBRANDT_SRC_NETWORK_UCX_ENDPOINT_H_
#define REMBRANDT_SRC_NETWORK_UCX_ENDPOINT_H_

#include <ucp/api/ucp.h>

#include "worker.h"

namespace UCP {
class Endpoint {
 public:
  Endpoint(Worker &worker, const ucp_ep_params_t *params);
  ~Endpoint();
  void RegisterRKey(void *rkey_buffer);
  ucp_rkey_h GetRKey() { return rkey_; };
  bool hasRKey() { return rkey_ != nullptr; };
  ucp_ep_h &GetEndpointHandle() { return ep_; };
 private:
  Worker &worker_;
  ucp_ep_h ep_;
  ucp_rkey_h rkey_ = nullptr;
};
}
#endif //REMBRANDT_SRC_NETWORK_UCX_ENDPOINT_H_
