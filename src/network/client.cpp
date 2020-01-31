#include "../../include/rembrandt/network/client.h"

#include <string.h>    /* memset */
#include <stdexcept>

#include "rembrandt/network/context.h"
#include "rembrandt/network/worker.h"
#include "rembrandt/network/utils.h"
#include "rembrandt/network/socket/static_client.h"

#define TEST_STRING_LEN sizeof(test_message)

/**
 * Error handling callback.
 */
static void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status) {
  printf("error handling callback was invoked with status %d (%s)\n",
         status, ucs_status_string(status));
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

struct sockaddr_in Client::CreateConnectionAddress(const char *address,
                                                   const uint16_t port) {
  struct sockaddr_in connect_addr;
  memset(&connect_addr, 0, sizeof(struct sockaddr_in));
  connect_addr.sin_family = AF_INET;
  connect_addr.sin_addr.s_addr = inet_addr(address);
  connect_addr.sin_port = htons(port);
  return connect_addr;
}

/**
 * Progress the request until it completes.
 */
static ucs_status_t request_wait(ucp_worker_h ucp_worker, test_req_t *request) {
  ucs_status_t status;

  /*  if operation was completed immediately */
  if (request == NULL) {
    return UCS_OK;
  }

  if (UCS_PTR_IS_ERR(request)) {
    return UCS_PTR_STATUS(request);
  }

  while (request->complete == 0) {
    ucp_worker_progress(ucp_worker);
  }
  status = ucp_request_check_status(request);

  /* This request may be reused so initialize it for next time */
  request->complete = 0;
  ucp_request_free(request);

  return status;
}

/**
 * Send and receive a message using the Stream API.
 * The client sends a message to the server and waits until the send it completed.
 * The server receives a message from the client and waits for its completion.
 */
static int send_recv_stream(ucp_worker_h ucp_worker,
                            ucp_ep_h ep,
                            int is_server) {
  char recv_message[TEST_STRING_LEN] = "";
  test_req_t *request;
  int ret = 0;
  ucs_status_t status;

  /* Client sends a message to the server using the stream API */
  request = (test_req_t *) ucp_stream_send_nb(ep, test_message, 1,
                                              ucp_dt_make_contig(TEST_STRING_LEN),
                                              stream_send_cb, 0);

  status = request_wait(ucp_worker, request);
  if (status != UCS_OK) {
    fprintf(stderr, "unable to %s UCX message (%s)\n",
            is_server ? "receive" : "send",
            ucs_status_string(status));
    ret = -1;
  } else {
    print_result(is_server, recv_message);
  }

  return ret;
}

/**
 * Close the given endpoint.
 * Currently closing the endpoint with UCP_EP_CLOSE_MODE_FORCE since we currently
 * cannot rely on the client side to be present during the server's endpoint
 * closing process.
 */
static void ep_close(ucp_worker_h ucp_worker, ucp_ep_h ep) {
  ucs_status_t status;
  void *close_req;

  close_req = ucp_ep_close_nb(ep, UCP_EP_CLOSE_MODE_FORCE);
  if (UCS_PTR_IS_PTR(close_req)) {
    do {
      ucp_worker_progress(ucp_worker);
      status = ucp_request_check_status(close_req);
    } while (status == UCS_INPROGRESS);

    ucp_request_free(close_req);
  } else if (UCS_PTR_STATUS(close_req) != UCS_OK) {
    fprintf(stderr, "failed to close ep %p\n", (void *) ep);
  }
}

Client::Client(ucp_context_h &ucp_context,
               ucp_worker_h &ucp_worker)
    : ucp_context_(ucp_context), ucp_worker_(ucp_worker) {}

ucp_ep_h Client::Connect(char *server_addr,
                         uint16_t port) {
  ucs_status_t status;
  ucp_ep_h ep;
  struct sockaddr_in connect_addr = CreateConnectionAddress(server_addr, port);
  ucp_ep_params_t ep_params = CreateParams(connect_addr);
  status = ucp_ep_create(ucp_worker_, &ep_params, &ep);
  if (status != UCS_OK) {
    throw
        std::runtime_error(std::string("failed to connect (%s)\n",
                                       ucs_status_string(status))
        );
  }
  return
      ep;
}

Client::~Client() {
  ucp_worker_destroy(ucp_worker_);
}

void Client::SendTest(ucp_ep_h &ep) {
  /* Client-Server communication via Stream API */
  send_recv_stream(ucp_worker_, ep, 0);
}

void Client::Disconnect(ucp_ep_h &ep) {
  ep_close(ucp_worker_, ep);
}

int main(int argc, char **argv) {
  char *IPADDRESS = (char *) "192.168.5.40";
  /* Initialize the UCX required objects */
  Context ucp_context = Context(true);
  Worker ucp_worker = Worker(ucp_context);

  /* Client side */
  Client client =
      Client(ucp_context.ucp_context_, ucp_worker.ucp_worker_);
  ucp_ep_h ep;
  ep = client.Connect(IPADDRESS, 13337);
  ucp_rkey_h rkey = client.RegisterRemoteMemory(ep, IPADDRESS, 13338);
  printf("Foo %p", (void *) rkey);
  client.SendTest(ep);
  /* Close the endpoint to the server */
  client.Disconnect(ep);
}

ucp_ep_params_t Client::CreateParams(struct sockaddr_in &connect_addr) {
  ucp_ep_params_t params;
  params.field_mask = UCP_EP_PARAM_FIELD_FLAGS |
      UCP_EP_PARAM_FIELD_SOCK_ADDR |
      UCP_EP_PARAM_FIELD_ERR_HANDLER |
      UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
  params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
  params.err_handler.cb = err_cb;
  params.err_handler.arg = NULL;
  params.flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
  params.sockaddr.addr = (struct sockaddr *) &connect_addr;
  params.sockaddr.addrlen = sizeof(connect_addr);
  return params;
}

ucp_rkey_h Client::RegisterRemoteMemory(ucp_ep_h ep,
                                        char *server_addr,
                                        __uint16_t rkey_port) {
  StaticClient static_client = StaticClient();
  static_client.Connect(server_addr, rkey_port);
  void *rkey_buffer;
  size_t rkey_size;
  static_client.ReceivePayload(&rkey_buffer, &rkey_size);
  ucp_rkey_h rkey;
  ucp_ep_rkey_unpack(ep, rkey_buffer, &rkey);
  return rkey;
}