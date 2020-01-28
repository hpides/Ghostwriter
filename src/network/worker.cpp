#include "rembrandt/network/worker.h"

#include <stdexcept>
#include <string.h>    /* memset */

Worker::Worker(Context &ucp_context) {
  /* UCP objects */
  ucp_worker_params_t worker_params;
  ucs_status_t status;

  memset(&worker_params, 0, sizeof(worker_params));

  worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;

  status = ucp_worker_create(ucp_context.ucp_context_,
                             &worker_params,
                             &ucp_worker_);
  if (status != UCS_OK) {
    throw std::runtime_error("failed to ucp_worker_create");
  }
//        TODO: Formatting for exception
//        fprintf(stderr, "failed to ucp_worker_create (%s)\n",
//            ucs_status_string(status));
}