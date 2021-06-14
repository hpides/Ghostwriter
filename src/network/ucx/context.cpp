#include <cstring>
#include <iostream>
#include <stdexcept>

#include <rembrandt/network/ucx/context.h>
#include <rembrandt/network/ucx/memory_region.h>
#include <rembrandt/network/ucx/worker.h>
#include <rembrandt/network/utils.h>

using namespace UCP;

Context::Context(bool enable_rma) : context_(nullptr) {
  ucp_params_t ucp_params;
  ucs_status_t status;

  memset(&ucp_params, 0, sizeof(ucp_params));

  /* UCP initialization */
  ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES |
      UCP_PARAM_FIELD_REQUEST_SIZE |
      UCP_PARAM_FIELD_REQUEST_INIT;

  ucp_params.features = UCP_FEATURE_STREAM | UCP_FEATURE_WAKEUP;
  if (enable_rma) {
    ucp_params.features = ucp_params.features | UCP_FEATURE_RMA | UCP_FEATURE_AMO64;
  }
  ucp_params.request_size = sizeof(test_req_t);
  ucp_params.request_init = request_init;

  status = ucp_init(&ucp_params, nullptr, &context_);
  if (status != UCS_OK) {
    throw std::runtime_error("failed to ucp_init");
  }
}

Context::~Context() {
  ucp_cleanup(context_);
}

std::unique_ptr<Worker> Context::CreateWorker() {
  return std::unique_ptr<Worker>(new Impl::Worker(*this));
}

std::unique_ptr<MemoryRegion> Context::RegisterStorageRegion(StorageRegion &storage_region) {
  return std::unique_ptr<MemoryRegion>(new MemoryRegion(*this, storage_region));
}