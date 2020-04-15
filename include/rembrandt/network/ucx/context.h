#ifndef REMBRANDT_SRC_NETWORK_UCX_CONTEXT_H_
#define REMBRANDT_SRC_NETWORK_UCX_CONTEXT_H_

extern "C" {
#include "ucp/api/ucp.h"
#include "ucs/type/status.h"
}
namespace UCP {
class Context {
 public:
  Context() = delete;
  explicit Context(bool enable_rma);
  ~Context();
  Context(const Context &other) = delete;
  Context(Context &&other) noexcept = delete;
  Context &operator=(const Context &) = delete;
  Context &operator=(Context &&other) noexcept = delete;
  ucp_context_h GetContextHandle() { return context_; };
 private:
  ucp_context_h context_;
};
}
#endif //REMBRANDT_SRC_NETWORK_UCX_CONTEXT_H_
