#ifndef REMBRANDT_SRC_NETWORK_UCX_ENDPOINT_H_
#define REMBRANDT_SRC_NETWORK_UCX_ENDPOINT_H_

#include <ucp/api/ucp.h>

#include "worker.h"

namespace UCP {
class Endpoint {
 public:
  Endpoint(Worker &worker, const ucp_ep_params_t *params);
  ~Endpoint();
  Endpoint(const Endpoint &) = delete;
  Endpoint &operator=(const Endpoint &) = delete;
  void RegisterRKey(void *rkey_buffer);
  ucp_rkey_h GetRKey() { return rkey_; };
  bool hasRKey() { return rkey_ != nullptr; };
  ucp_ep_h GetHandle() {return ep_;};
  uint64_t GetRemoteAddress() { return remote_addr_; };
  ucs_status_ptr_t receive(void *buffer, size_t length, size_t *received_length);
  ucs_status_ptr_t send(const void *buffer, size_t length);
  ucs_status_ptr_t put(const void *buffer,
                       size_t length,
                       uint64_t remote_addr,
                       ucp_send_callback_t cb);
  ucs_status_ptr_t get(void *buffer,
                       size_t length,
                       uint64_t remote_addr,
                       ucp_send_callback_t cb);
 private:
  Worker &worker_;
  ucp_ep_h ep_;
  ucp_rkey_h rkey_;
  uint64_t remote_addr_;
};
}
#endif //REMBRANDT_SRC_NETWORK_UCX_ENDPOINT_H_
