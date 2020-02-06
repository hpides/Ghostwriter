#include "rembrandt/network/test_server.h"


void TestServer::Listen() {
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
      /* Client-Server communication via Stream API */
      receive_stream(worker_.GetWorkerHandle(), server_context_.ep);

      /* Close the endpoint to the client */
      endpoint_.reset();

      /* Initialize server's endpoint for the next connection with a new
       * client */
      printf("Waiting for connection...\n");
    };
  }
}


int main(int argc, char *argv[]) {
  /* Initialize the UCX required objects */
  UCP::Context context = UCP::Context(true);
  Server server = Server(context);
  server.Listen();
}