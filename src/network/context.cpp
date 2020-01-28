#include "../../include/rembrandt/network/context.h"

#include <cstring>
#include <stdexcept>

#include "rembrandt/network/utils.h"

Context::Context() {
  Context(true);
}

Context::Context(bool enable_rma) {
  /* UCP objects */
//  ucp_worker_params_t worker_params;
  ucp_params_t ucp_params;
  ucs_status_t status;

  memset(&ucp_params, 0, sizeof(ucp_params));
//  memset(&worker_params, 0, sizeof(worker_params));

  /* UCP initialization */
  ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES |
      UCP_PARAM_FIELD_REQUEST_SIZE |
      UCP_PARAM_FIELD_REQUEST_INIT;

  if (enable_rma) {
    ucp_params.features = UCP_FEATURE_STREAM |
        UCP_FEATURE_RMA;
  } else {
    ucp_params.features = UCP_FEATURE_STREAM;
  }
  ucp_params.request_size = sizeof(test_req_t);
  ucp_params.request_init = request_init;

  status = ucp_init(&ucp_params, NULL, &ucp_context_);
  if (status != UCS_OK) {
    // TODO: String formatting for exception
    throw std::runtime_error("failed to ucp_init");//std::string(fprints("failed to ucp_init (%s)\n", ucs_status_string(status)));
  }
}

Context::~Context() {
  ucp_cleanup(ucp_context_);
}
