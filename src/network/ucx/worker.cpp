#include <stdexcept>
#include <string.h>    /* memset */
#include <rembrandt/network/ucx/worker.h>

using namespace UCP;

Worker::Worker(Context &context) {
  /* UCP objects */
  ucp_worker_params_t worker_params;
  ucs_status_t status;

  memset(&worker_params, 0, sizeof(worker_params));

  worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  worker_params.thread_mode = UCS_THREAD_MODE_SERIALIZED;

  status = ucp_worker_create(context.GetContextHandle(),
                             &worker_params,
                             &worker_);
  if (status != UCS_OK) {
    throw std::runtime_error("failed to ucp_worker_create");
  }
//        TODO: Formatting for exception
}

Worker::~Worker() {
  printf("Destroyed worker %p\n", (void *) worker_);
  ucp_worker_destroy(worker_);
}
