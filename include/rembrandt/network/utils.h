#ifndef REMBRANDT_SRC_NETWORK_UTILS_H_
#define REMBRANDT_SRC_NETWORK_UTILS_H_

#include <cstddef>

extern "C" {
#include <ucp/api/ucp.h>
}

const char test_message[] = "UCX Client-Server Hello World";

#define TEST_STRING_LEN sizeof(test_message)

/**
 * Server context to be used in the user's accept callback.
 * It holds the server's endpoint which will be created upon accepting a
 * connection request from the client.
 */
typedef struct ucx_server_ctx {
  ucp_ep_h ep;
} ucx_server_ctx_t;

/**
 * Stream request context. Holds a value to indicate whether or not the
 * request is completed.
 */
typedef struct test_req {
  int complete;
} test_req_t;

/**
 * A callback to be invoked by UCX in order to initialize the user's request.
 */
void request_init(void *request);

/**
 * Print the received message on the server side or the sent data on the client
 * side.
 */
void print_result(int is_server, char *recv_message);

void empty_cb(void *request, ucs_status_t status);

void empty_stream_recv_cb(void *request, ucs_status_t status, size_t length);

void print_cb(void *request, ucs_status_t status);

#endif //REMBRANDT_SRC_NETWORK_UTILS_H_

