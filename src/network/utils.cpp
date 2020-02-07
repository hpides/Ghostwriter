#include <iostream>
#include "../../include/rembrandt/network/utils.h"

// TODO: FIX ISSUE
void request_init(void *request) {
  test_req_t *req = (test_req_t *) request;
  req->complete = 0;
}

void print_result(int is_server, char *recv_message) {
  if (is_server) {
    printf("UCX data message was received\n");
    printf("\n\n----- UCP TEST SUCCESS -------\n\n");
    printf("%s", recv_message);
    printf("\n\n------------------------------\n\n");
  } else {
    printf("\n\n-----------------------------------------\n\n");
    printf("Client sent message: \n%s.\nlength: %ld\n",
           test_message, TEST_STRING_LEN);
    printf("\n-----------------------------------------\n\n");
  }
}

void print_cb(void *request, ucs_status_t status) {
  std::cout << "CB triggered.\n";
}

void empty_cb(void *request, ucs_status_t status) {
}