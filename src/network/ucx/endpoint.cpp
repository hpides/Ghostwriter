#include <stdexcept>
#include "../../../include/rembrandt/network/ucx/endpoint.h"

using namespace UCP;

Endpoint::Endpoint(Worker &worker, const ucp_ep_params_t *params) :
    worker_(worker) {
  ucs_status_t status;
  status = ucp_ep_create(worker.GetWorkerHandle(), params, &ep_);
  if (status != UCS_OK) {
    throw
        std::runtime_error(std::string("failed to connect (%s)\n",
                                       ucs_status_string(status))
        );
  }
}

Endpoint::~Endpoint() {
  /**
 * Close the given endpoint.
 * Currently closing the endpoint with UCP_EP_CLOSE_MODE_FORCE since we currently
 * cannot rely on the client side to be present during the server's endpoint
 * closing process.
 */
  ucs_status_t status;
  void *close_req;

  close_req = ucp_ep_close_nb(ep_, UCP_EP_CLOSE_MODE_FORCE);
  if (UCS_PTR_IS_PTR(close_req)) {
    do {
      worker_.Progress();
      status = ucp_request_check_status(close_req);
    } while (status == UCS_INPROGRESS);

    ucp_request_free(close_req);
  } else if (UCS_PTR_STATUS(close_req) != UCS_OK) {
    // TODO: Throw error
    fprintf(stderr, "failed to close ep %p\n", (void *) ep_);
  }
  // TODO: Handle remote memory key better
  if (hasRKey()) {
    ucp_rkey_destroy(rkey_);
  }
}

void Endpoint::RegisterRKey(void *rkey_buffer) {
  ucp_ep_rkey_unpack(ep_, rkey_buffer, &rkey_);
  // TODO: Handle status
}