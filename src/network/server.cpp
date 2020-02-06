#include <stdexcept>
#include <string.h>    /* memset */

#include <arpa/inet.h> /* inet_addr */

#include <thread>
#include <rembrandt/network/server.h>
#include <rembrandt/network/utils.h>
#include <rembrandt/protocol/rembrandt_protocol_generated.h>

Server::Server(UCP::Context &context, uint16_t port, uint16_t rkey_port)
    : context_(context),
      worker_(UCP::Worker(context)),
      memory_region_(UCP::MemoryRegion(context)) {
  StartRKeyServer(rkey_port);
  StartListener(port);
}
void Server::StartListener(uint16_t port) {/* Initialize the server's endpoint to NULL. Once the server's endpoint
* is created, this field will have a valid value. */
  ucs_status_t status;
  server_context_.ep = NULL;
  struct sockaddr_in listen_addr = CreateListenAddress(port);
  ucp_listener_params_t params = CreateListenerParams(&listen_addr);

  /* Create a listener on the server side to listen on the given address.*/
  status =
      ucp_listener_create(worker_.GetWorkerHandle(), &params, &ucp_listener_);
  if (status != UCS_OK) {
    throw std::runtime_error(std::string("failed to create listener (%s)\n",
                                         ucs_status_string(status)));
  }
}

sockaddr_in Server::CreateListenAddress(uint16_t port) {
  sockaddr_in listen_addr;
  memset(&listen_addr, 0, sizeof(struct sockaddr_in));
  listen_addr.sin_family = AF_INET;
  listen_addr.sin_addr.s_addr = INADDR_ANY;
  listen_addr.sin_port = htons(port);
  return listen_addr;
}

ucp_listener_params_t Server::CreateListenerParams(sockaddr_in *listen_addr) {
  ucp_listener_params_t params;
  params.field_mask = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
      UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;
  params.sockaddr.addr = (const struct sockaddr *) listen_addr;
  params.sockaddr.addrlen = sizeof(*listen_addr);
  params.conn_handler.cb = server_conn_req_cb;
  params.conn_handler.arg = this;
  return params;
}

/**
 * Error handling callback.
 */
static void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status) {
  printf("error handling callback was invoked with status %d (%s)\n",
         status, ucs_status_string(status));
}

ucp_ep_params_t Server::CreateEndpointParams(ucp_conn_request_h conn_request) {
  ucp_ep_params_t params;
  params.field_mask = UCP_EP_PARAM_FIELD_FLAGS |
      UCP_EP_PARAM_FIELD_CONN_REQUEST |
      UCP_EP_PARAM_FIELD_ERR_HANDLER |
      UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
  params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
  params.err_handler.cb = err_cb;
  params.err_handler.arg = NULL;
  params.conn_request = conn_request;
  return params;
}

void Server::StartRKeyServer(uint16_t rkey_port) {
  void *rkey_buffer;
  size_t rkey_size;
  memory_region_.Pack(&rkey_buffer, &rkey_size);
  char *data = (char *) malloc(sizeof(uint64_t) + rkey_size);
  uint64_t region_ptr = (uint64_t) reinterpret_cast<uintptr_t>(memory_region_.region_);
  memcpy(data, &region_ptr, sizeof(uint64_t));
  memcpy(data + sizeof(uint64_t), rkey_buffer, rkey_size);
  ucp_rkey_buffer_release(rkey_buffer);
  std::shared_ptr<void>
      rkey_shared_ptr = std::shared_ptr<void>(data);
  rkey_server_.SetPayload(rkey_shared_ptr, sizeof(uint64_t) + rkey_size);
  rkey_server_.Run(rkey_port);
}

void Server::Listen() {
  /* Server is always up */
  printf("Waiting for connection...\n");
  while (1) {
    /* Wait for the server's callback to set the context->ep field, thus
     * indicating that the server's endpoint was created and is ready to
     * be used. The client side should initiate the connection, leading
     * to this ep's creation */
    if (!endpoint_) {
      worker_.Progress();
    } else {
      worker_.Progress();

//      /* Client-Server communication via Stream API */
//      receive_stream(worker_.GetWorkerHandle(), server_context_.ep);
//
//      /* Close the endpoint to the client */
//      endpoint_.reset();
//
//      /* Initialize server's endpoint for the next connection with a new
//       * client */
//      printf("Waiting for connection...\n");
    };
  }
}

/**
 * Progress the request until it completes.
 */
static ucs_status_t request_wait(ucp_worker_h ucp_worker,
                                 test_req_t *request) {
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

static uint16_t receive_message_size(ucp_worker_h worker, ucp_ep_h) {

}

//static Rembrandt::Protocol::BaseMessage receive_message(ucp_worker_h worker,
//                                                        ucp_ep_h ep) {
//  uint16_t length;
//  test_req_t *request;
//  request = (test_req_t *) ucp_stream_recv_nb(ep,
//                                              &length,
//                                              1,
//                                              datatype,
//                                              stream_recv_cb,
//                                              sizeof(length),
//                                              UCP_STREAM_RECV_FLAG_WAITALL);
//  ucs_status_t status = request_wait(worker, request);
//  if (status != UCS_OK) {
//    fprintf(stderr,
//            "unable to receive UCX message (%s)\n",
//            ucs_status_string(status)
//    );
//    ret = -1;
//  } else {
//    print_result(1, recv_message);
//  }
//  return
//}
//
///**
// * Send and receive a message using the Stream API.
// * The client sends a message to the server and waits until the send it completed.
// * The server receives a message from the client and waits for its completion.
// */
//static int receive_stream(ucp_worker_h ucp_worker,
//                          ucp_ep_h ep) {
//  char recv_message[TEST_STRING_LEN] = "";
//  test_req_t *request;
//  size_t length;
//  int ret = 0;
//  ucs_status_t status;
///* Server receives a message from the client using the stream API */
//  request = (test_req_t *) ucp_stream_recv_nb(ep, &recv_message, 1,
//                                              ucp_dt_make_contig(
//                                                  TEST_STRING_LEN),
//                                              stream_recv_cb, &length,
//                                              UCP_STREAM_RECV_FLAG_WAITALL);
//  status = request_wait(ucp_worker, request);
//  if (status != UCS_OK) {
//    fprintf(stderr,
//            "unable to receive UCX message (%s)\n",
//            ucs_status_string(status)
//    );
//    ret = -1;
//  } else {
//    print_result(1, recv_message);
//  }
//
//  return
//      ret;
//}

/**
 * The callback on the receiving side, which is invoked upon receiving the
 * stream message.
 */
static void stream_recv_cb(void *request,
                           ucs_status_t status,
                           size_t length) {
  test_req_t *req = (test_req_t *) request;

  req->complete = 1;

  printf("stream_recv_cb returned with status %d (%s), length: %lu\n",
         status, ucs_status_string(status), length);
}

void Server::CreateServerEndpoint(ucp_conn_request_h conn_request) {
  const ucp_ep_params_t params = CreateEndpointParams(conn_request);
  endpoint_ = std::make_unique<UCP::Endpoint>(worker_, &params);
}

static void server_conn_req_cb(ucp_conn_request_h conn_request, void *arg) {
  Server *server = (Server *) arg;
  server->CreateServerEndpoint(conn_request);
}
