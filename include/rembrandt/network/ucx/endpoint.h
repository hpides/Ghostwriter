#ifndef REMBRANDT_SRC_NETWORK_UCX_ENDPOINT_H_
#define REMBRANDT_SRC_NETWORK_UCX_ENDPOINT_H_

#include <ucp/api/ucp.h>

#include "worker.h"

namespace UCP {
class Endpoint {
 public:
  virtual ~Endpoint() = 0;
  virtual void RegisterRMemInfo(const std::string &remote_key, uint64_t remote_addr) = 0;
  virtual ucp_rkey_h GetRKey() const = 0;
  virtual bool hasRKey() const = 0;
  virtual ucp_ep_h GetHandle() const = 0;
  virtual uint64_t GetRemoteAddress() const = 0;
  virtual ucs_status_ptr_t flush(ucp_send_callback_t cb) const = 0;
  virtual ucs_status_ptr_t receive(void *buffer, size_t length, size_t *received_length) const = 0;
  virtual ucs_status_ptr_t send(const void *buffer, size_t length) const = 0;
  virtual ucs_status_ptr_t put(const void *buffer,
                               size_t length,
                               uint64_t remote_addr,
                               ucp_send_callback_t cb) const = 0;
  virtual ucs_status_ptr_t get(void *buffer,
                               size_t length,
                               uint64_t remote_addr,
                               ucp_send_callback_t cb) const = 0;

  virtual ucs_status_ptr_t CompareAndSwap(uint64_t compare,
                                          void *swap,
                                          size_t op_size,
                                          uint64_t remote_addr,
                                          ucp_send_callback_t cb) const = 0;
};

namespace Impl {
class Endpoint : public UCP::Endpoint {
 public:
  Endpoint() = delete;
  Endpoint(UCP::Worker &worker, const ucp_ep_params_t *params);
  virtual ~Endpoint();
  Endpoint(const Endpoint &) = delete;
  Endpoint(Endpoint &&other) noexcept = delete;
  Endpoint &operator=(const Endpoint &) = delete;
  Endpoint &operator=(Endpoint &&other) noexcept = delete;
  void RegisterRMemInfo(const std::string &remote_key, uint64_t remote_addr) override;
  ucp_rkey_h GetRKey() const override { return rkey_; };
  bool hasRKey() const override { return rkey_ != nullptr; };
  ucp_ep_h GetHandle() const override { return ep_; };
  uint64_t GetRemoteAddress() const override { return remote_addr_; };
  ucs_status_ptr_t flush(ucp_send_callback_t cb) const override;
  ucs_status_ptr_t receive(void *buffer, size_t length, size_t *received_length) const override;
  ucs_status_ptr_t send(const void *buffer, size_t length) const override;
  ucs_status_ptr_t put(const void *buffer,
                       size_t length,
                       uint64_t remote_addr,
                       ucp_send_callback_t cb) const override;
  ucs_status_ptr_t get(void *buffer,
                       size_t length,
                       uint64_t remote_addr,
                       ucp_send_callback_t cb) const override;

  ucs_status_ptr_t CompareAndSwap(uint64_t compare,
                                  void *swap,
                                  size_t op_size,
                                  uint64_t remote_addr,
                                  ucp_send_callback_t cb) const override;
 private:
  UCP::Worker &worker_;
  ucp_ep_h ep_;
  ucp_rkey_h rkey_;
  uint64_t remote_addr_;
};
}
}
#endif //REMBRANDT_SRC_NETWORK_UCX_ENDPOINT_H_
