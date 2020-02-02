#include "../../../include/rembrandt/network/ucx/test_client.h"

void TestClient::SendTest(ucp_ep_h &ep) {
  /* Client-Server communication via Stream API */
  send_recv_stream(ucp_worker_, ep, 0);
}
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
