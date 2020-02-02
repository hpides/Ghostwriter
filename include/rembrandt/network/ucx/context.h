#ifndef REMBRANDT_SRC_NETWORK_UCX_CONTEXT_H_
#define REMBRANDT_SRC_NETWORK_UCX_CONTEXT_H_

extern "C" {
#include "ucp/api/ucp.h"
#include "ucs/type/status.h"
}
namespace UCP {
class Context {
 public:
  Context();
  Context(bool enable_rma);
  ~Context();
  ucp_context_h GetContextHandle() { return context_; };
 private:
  ucp_context_h context_;
};
}
#endif //REMBRANDT_SRC_NETWORK_UCX_CONTEXT_H_
