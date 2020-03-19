#include <ucs/type/status.h>
#include <ucp/api/ucp.h>
#include "rembrandt/network/request_processor.h"

RequestProcessor::RequestProcessor(UCP::Worker &worker) : worker_(worker) {}

ucs_status_t RequestProcessor::Process(void *status_ptr) {
  if (status_ptr == nullptr) {
    return UCS_OK;
  }
  ucs_status_t status;
  if (UCS_PTR_IS_ERR(status_ptr)) {
    status = ucp_request_check_status(status_ptr);
    ucp_request_free(status_ptr);
    return status;
  }
  do {
    worker_.Progress();
    status = ucp_request_check_status(status_ptr);
  } while (status == UCS_INPROGRESS);

  /* This request may be reused so initialize it for next time */
  ucp_request_free(status_ptr);
  return status;
}