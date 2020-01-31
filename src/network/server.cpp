#include "../../include/rembrandt/network/server.h"

#include <iostream>
#include <stdexcept>
#include <string.h>    /* memset */

#include <arpa/inet.h> /* inet_addr */

#include "rembrandt/network/context.h"
#include "rembrandt/network/worker.h"

#include <thread>

sockaddr_in Server::CreateListenAddress(uint16_t port) {
  sockaddr_in listen_addr;
  memset(&listen_addr, 0, sizeof(struct sockaddr_in));
  listen_addr.sin_family = AF_INET;
  listen_addr.sin_addr.s_addr = INADDR_ANY;
  listen_addr.sin_port = htons(port);
  return listen_addr;
}

ucp_listener_params_t Server::CreateParams(sockaddr_in *listen_addr) {
  ucp_listener_params_t params;
  params.field_mask = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
      UCP_LISTENER_PARAM_FIELD_ACCEPT_HANDLER;
  params.sockaddr.addr = (const struct sockaddr *) listen_addr;
  params.sockaddr.addrlen = sizeof(*listen_addr);
  params.accept_handler.cb = server_accept_cb;
  params.accept_handler.arg = &context_;
  return params;
}

void Server::StartRKeyServer(uint16_t rkey_port) {
  void *rkey_buffer;
  size_t rkey_size;
  memory_region_.Pack(&rkey_buffer, &rkey_size);
  std::shared_ptr<void>
      rkey_shared_ptr = std::shared_ptr<void>((char *) rkey_buffer);
  rkey_server_.SetPayload(rkey_shared_ptr, rkey_size);
  rkey_server_.Run(rkey_port);
}

Server::Server(ucp_context_h &ucp_context,
               ucp_worker_h &ucp_worker,
               uint16_t port,
               uint16_t rkey_port)
    : ucp_context_(ucp_context),
      ucp_worker_(ucp_worker),
      memory_region_(MemoryRegion(ucp_context)) {
  void *rkey_buffer;
  size_t rkey_size;
  memory_region_.Pack(&rkey_buffer, &rkey_size);
  rkey_server_.SetPayload(std::shared_ptr<char>((char *) rkey_buffer), rkey_size);
  rkey_server_.Run(rkey_port);
  /* Initialize the server's endpoint to NULL. Once the server's endpoint
 * is created, this field will have a valid value. */
  ucs_status_t status;
  try {
    context_.ep = NULL;
    struct sockaddr_in listen_addr = CreateListenAddress(port);
    ucp_listener_params_t params = CreateParams(&listen_addr);

    /* Create a listener on the server side to listen on the given address.*/
    status = ucp_listener_create(ucp_worker, &params, &ucp_listener_);
    if (status != UCS_OK) {
      throw std::runtime_error(std::string("failed to listen (%s)\n",
                                           ucs_status_string(status)));
    }
  } catch (const std::runtime_error &e) {
    std::cerr << e.what() << "\n";
    ucp_worker_destroy(ucp_worker);
    ucp_cleanup(ucp_context);
    throw std::runtime_error(std::string("failed to start server\n"));
  }
}

void Server::Listen() {
  /* Server is always up */
  printf("Waiting for connection...\n");
  while (1) {
    /* Wait for the server's callback to set the context->ep field, thus
     * indicating that the server's endpoint was created and is ready to
     * be used. The client side should initiate the connection, leading
     * to this ep's creation */
    if (context_.ep == NULL) {
      ucp_worker_progress(ucp_worker_);
    } else {
      /* Client-Server communication via Stream API */
      receive_stream(ucp_worker_, context_.ep);

      /* Close the endpoint to the client */
      ep_close(ucp_worker_, context_.ep);

      /* Initialize server's endpoint for the next connection with a new
       * client */
      context_.ep = NULL;
      printf("Waiting for connection...\n");
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

/**
 * Send and receive a message using the Stream API.
 * The client sends a message to the server and waits until the send it completed.
 * The server receives a message from the client and waits for its completion.
 */
static int receive_stream(ucp_worker_h ucp_worker,
                          ucp_ep_h ep) {
  char recv_message[TEST_STRING_LEN] = "";
  test_req_t *request;
  size_t length;
  int ret = 0;
  ucs_status_t status;
/* Server receives a message from the client using the stream API */
  request = (test_req_t *) ucp_stream_recv_nb(ep, &recv_message, 1,
                                              ucp_dt_make_contig(
                                                  TEST_STRING_LEN),
                                              stream_recv_cb, &length,
                                              UCP_STREAM_RECV_FLAG_WAITALL);
  status = request_wait(ucp_worker, request);
  if (status != UCS_OK) {
    fprintf(stderr,
            "unable to receive UCX message (%s)\n",
            ucs_status_string(status)
    );
    ret = -1;
  } else {
    print_result(1, recv_message);
  }

  return
      ret;
}

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

/**
 * The callback on the server side which is invoked upon receiving a connection
 * request from the client.
 */
static void server_accept_cb(ucp_ep_h ep, void *arg) {
  ucx_server_ctx_t *context = (ucx_server_ctx_t *) arg;

  /* Save the server's endpoint in the user's context, for future usage */
  context->ep = ep;
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

int main(int argc, char *argv[]) {
  /* Initialize the UCX required objects */
  Context ucp_context = Context(true);
  Worker ucp_worker = Worker(ucp_context);
  Server server =
      Server(ucp_context.ucp_context_, ucp_worker.ucp_worker_);
  server.Listen();
}