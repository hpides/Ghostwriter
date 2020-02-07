#include <rembrandt/network/utils.h>
#include <iostream>
#include "rembrandt/consumer/consumer.h"

Consumer::Consumer(UCP::Context &context) : client_(context) {
  char *IPADDRESS = (char *) "192.168.5.31";
  UCP::Endpoint &ep = client_.GetConnection(IPADDRESS, 13350);
  ucp_rkey_h rkey = client_.RegisterRemoteMemory(ep, IPADDRESS, 13351);
};

void *Consumer::Receive(TopicPartition topic_partition,
                        uint64_t offset,
                        size_t max_length) {
  void *buffer = malloc(max_length);
  // TODO: Use concrete endpoints
  UCP::Endpoint &ep = client_.GetConnection("", 0);
  ucs_status_ptr_t status_ptr = ep.put(buffer, max_length, offset, print_cb);

  if (status_ptr == NULL) {
    std::cout << "Request completed immediately\n";
    return buffer;
  }

  if (UCS_PTR_IS_ERR(status_ptr)) {
    throw std::runtime_error("Runtime error!\n");
  }
  ucs_status_t status;
  do {
    ucp_worker_progress(client_.GetWorker().GetWorkerHandle());
    status = ucp_request_check_status(status_ptr);
  } while (status == UCS_INPROGRESS);

  /* This request may be reused so initialize it for next time */
  ucp_request_free(status_ptr);

  // TODO: Handle errors
  if (status != UCS_OK) {
    throw std::runtime_error("Failed receiving\n");
  }
  std::cout << "Done receiving\n";

  return buffer;
}

int main(int argc, char *argv[]) {
  UCP::Context context = UCP::Context(true);
  Consumer consumer = Consumer(context);
  TopicPartition topic_partition(1, 1);
  for (uint32_t i = 0; i < 100; i += 16 / sizeof(uint32_t)) {
    void *buffer = consumer.Receive(topic_partition, i, 16);
    for (int i = 0; i < 16 / sizeof(uint32_t); i++) {
      std::cout << ((uint32_t *) buffer)[i] << "\n";
    }
    free(buffer);
  }
}