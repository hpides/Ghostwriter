#ifndef REMBRANDT_SRC_NETWORK_CONTEXT_H_
#define REMBRANDT_SRC_NETWORK_CONTEXT_H_

extern "C" {
#include <ucp/api/ucp.h>
#include <ucs/type/status.h>
}

class Context {
 public:
  Context();
  Context(bool enable_rma);
  ~Context();
  ucp_context_h ucp_context_;
};

#endif //REMBRANDT_SRC_NETWORK_CONTEXT_H_
