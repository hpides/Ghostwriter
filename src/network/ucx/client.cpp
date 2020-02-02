#include "rembrandt/network/ucx/client.h"

#include <string.h>    /* memset */
#include <stdexcept>
#include "rembrandt/network/ucx/context.h"
#include "rembrandt/network/ucx/worker.h"
#include "rembrandt/network/utils.h"
#include "rembrandt/network/socket/static_client.h"

using namespace UCP;

Client::Client(Context &context) : context_(context),
                                   worker_(Worker(context)) {}

struct sockaddr_in Client::CreateConnectionAddress(const char *address,
                                                   const uint16_t port) {
  struct sockaddr_in connect_addr;
  memset(&connect_addr, 0, sizeof(struct sockaddr_in));
  connect_addr.sin_family = AF_INET;
  connect_addr.sin_addr.s_addr = inet_addr(address);
  connect_addr.sin_port = htons(port);
  return connect_addr;
}

void Client::Connect(char *server_addr,
                     uint16_t port) {
  struct sockaddr_in connect_addr = CreateConnectionAddress(server_addr, port);
  ucp_ep_params_t ep_params = CreateParams(connect_addr);
  endpoint_ = std::make_shared<Endpoint>(worker_, &ep_params);
//  endpoints_[connect_addr] = std::make_shared<Endpoint>(worker_, &ep_params);
}

Endpoint &Client::GetConnection(char *server_addr, uint16_t port) {
//  struct sockaddr_in connect_addr = CreateConnectionAddress(server_addr, port);
  if (!endpoint_) {
    Connect(server_addr, port);
  }
//  if (endpoints_.find(std::pair(server_addr, port)) == endpoints_.end()) {
//    Connect(server_addr, port);
//  }
// TODO: Generalize
  return *endpoint_;
//  return *(endpoints_.at(std::pair(server_addr, port)).get());
}

void Client::Disconnect(char *server_addr, uint16_t port) {
//  struct sockaddr_in connect_addr = CreateConnectionAddress(server_addr, port);
//  endpoints_.erase(std::pair(server_addr, port));
// TODO: Generalize
  endpoint_.reset();
}

/**
 * Error handling callback.
 */
static void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status) {
  printf("error handling callback was invoked with status %d (%s)\n",
         status, ucs_status_string(status));
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

ucp_rkey_h Client::RegisterRemoteMemory(Endpoint endpoint,
                                        char *server_addr,
                                        __uint16_t rkey_port) {
  StaticClient static_client = StaticClient();
  static_client.Connect(server_addr, rkey_port);
  void *rkey_buffer;
  size_t rkey_size;
  static_client.ReceivePayload(&rkey_buffer, &rkey_size);
  endpoint.RegisterRKey(rkey_buffer);
  return endpoint.GetRKey();
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
 * The callback on the sending side, which is invoked after finishing sending
 * the stream message.
 */
static void stream_send_cb(void *request, ucs_status_t status) {
  test_req_t *req = (test_req_t *) request;

  req->complete = 1;

  printf("stream_send_cb returned with status %d (%s)\n",
         status, ucs_status_string(status));
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

void Client::SendTest(ucp_ep_h &ep) {
  /* Client-Server communication via Stream API */
  send_recv_stream(worker_.GetWorkerHandle(), ep, 0);
}
#define TEST_STRING_LEN sizeof(test_message)

int main(int argc, char **argv) {
  char *IPADDRESS = (char *) "192.168.5.40";
  /* Initialize the UCX required objects */
  Context context = Context(true);
  Worker worker = Worker(context);

  /* Client side */
  Client client = Client(context);
  Endpoint ep = client.GetConnection(IPADDRESS, 13337);
//  ucp_rkey_h rkey = client.RegisterRemoteMemory(ep, IPADDRESS, 13338);
  client.SendTest(ep.GetEndpointHandle());
  /* Close the endpoint to the server */
  client.Disconnect(IPADDRESS, 13337);
}
