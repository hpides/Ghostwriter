#include <stdexcept>
#include <rembrandt/network/ucx/endpoint.h>
#include <rembrandt/network/utils.h>

using namespace UCP;

Endpoint::~Endpoint() {}

Impl::Endpoint::Endpoint(UCP::Worker &worker, const ucp_ep_params_t *params) :
    worker_(worker),
    rkey_(nullptr) {
  ucs_status_t status;
  status = ucp_ep_create(worker.GetWorkerHandle(), params, &ep_);
  if (status != UCS_OK) {
    throw
        std::runtime_error(std::string("failed to connect (%s)\n",
                                       ucs_status_string(status))
        );
  }
}

Impl::Endpoint::~Endpoint() {
  /**
 * Close the given endpoint.
 * Currently closing the endpoint with UCP_EP_CLOSE_MODE_FORCE since we currently
 * cannot rely on the client side to be present during the server's endpoint
 * closing process.
 */
  ucs_status_t status;
  void *close_req;

  printf("Destroyed endpoint %p\n", (void *) ep_);
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

void Impl::Endpoint::RegisterRMemInfo(const std::string &remote_key, uint64_t remote_addr) {
  remote_addr_ = remote_addr;
  ucs_status_t
      ret = ucp_ep_rkey_unpack(ep_, remote_key.c_str(), &rkey_);
  printf("%d\n", ret);
  // TODO: Handle status
}

ucs_status_ptr_t Impl::Endpoint::flush(ucp_send_callback_t cb) const {
  return ucp_ep_flush_nb(ep_, 0, cb);
}

ucs_status_ptr_t Impl::Endpoint::receive(void *buffer, size_t length, size_t *received_length) const {
  return ucp_stream_recv_nb(ep_,
                            buffer,
                            1,
                            ucp_dt_make_contig(length),
                            empty_stream_recv_cb,
                            received_length,
                            UCP_STREAM_RECV_FLAG_WAITALL);
}

/**
 * The callback on the sending side, which is invoked after finishing sending
 * the stream message.
 */
static void stream_send_cb(void *request, ucs_status_t status) {
  test_req_t *req = (test_req_t *) request;

  req->complete = 1;

  printf("stream_send_cb returned with status %d (%s)\n",
         status, ucs_status_string(status));
}
ucs_status_ptr_t Impl::Endpoint::send(const void *buffer, size_t length) const {
  return ucp_stream_send_nb(ep_,
                            buffer,
                            1,
                            ucp_dt_make_contig(length),
                            stream_send_cb,
                            0);
}

ucs_status_ptr_t Impl::Endpoint::put(const void *buffer,
                                     size_t length,
                                     uint64_t remote_addr,
                                     ucp_send_callback_t cb) const {
  return ucp_put_nb(ep_, buffer, length, remote_addr, rkey_, cb);
}

ucs_status_ptr_t Impl::Endpoint::get(void *buffer,
                                     size_t length,
                                     uint64_t remote_addr,
                                     ucp_send_callback_t cb) const {
  return ucp_get_nb(ep_, buffer, length, remote_addr, rkey_, cb);
}

ucs_status_ptr_t Impl::Endpoint::CompareAndSwap(uint64_t compare,
                                                void *swap,
                                                size_t op_size,
                                                uint64_t remote_addr,
                                                ucp_send_callback_t cb) const {
  return ucp_atomic_fetch_nb(ep_, UCP_ATOMIC_FETCH_OP_CSWAP, compare, swap, op_size, remote_addr, rkey_, cb);
}

ucs_status_ptr_t Impl::Endpoint::Swap(uint64_t value,
                                      void *result,
                                      size_t op_size,
                                      uint64_t remote_addr,
                                      ucp_send_callback_t cb) const {
  return ucp_atomic_fetch_nb(ep_, UCP_ATOMIC_FETCH_OP_SWAP, value, result, op_size, remote_addr, rkey_, cb);
}
