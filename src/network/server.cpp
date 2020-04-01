#include <iostream>
#include <stdexcept>
#include <string.h>    /* memset */

#include <arpa/inet.h> /* inet_addr */
#include <unistd.h>
#include <thread>
#include <rembrandt/network/server.h>
#include <rembrandt/network/utils.h>
#include <rembrandt/protocol/flatbuffers/rembrandt_protocol_generated.h>
#include <rembrandt/network/message_handler.h>
#include <rembrandt/network/detached_message.h>

Server::Server(UCP::Context &context, uint16_t port)
    : context_(context),
      worker_(UCP::Worker(context)) {
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
  memset(&params, 0, sizeof(ucp_ep_params_t));
  params.field_mask = UCP_EP_PARAM_FIELD_FLAGS |
      UCP_EP_PARAM_FIELD_SOCK_ADDR |
      UCP_EP_PARAM_FIELD_CONN_REQUEST |
      UCP_EP_PARAM_FIELD_ERR_HANDLER |
      UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
  params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
  params.err_handler.cb = err_cb;
  params.err_handler.arg = NULL;
  params.conn_request = conn_request;
  return params;
}

void Server::Listen(MessageHandler *message_handler) {
  message_handler_ = message_handler;
  /* Server is always up */
  printf("Waiting for connection...\n");
  unsigned int progress;
  while (1) {
    /* Wait for the server's callback to set the context->ep field, thus
     * indicating that the server's endpoint was created and is ready to
     * be used. The client side should initiate the connection, leading
     * to this ep's creation */
    if (!endpoint_) {
      progress = worker_.Progress();
      if (!progress) {
        worker_.Wait();
      }
    } else {
      std::unique_ptr<Message> request = ReceiveMessage();
      std::unique_ptr<Message> response = message_handler_->HandleMessage(*request);
      if (!response->IsEmpty()) {
        ucs_status_ptr_t status_ptr = endpoint_->send(response->GetBuffer(), response->GetSize());
        ucs_status_t status = Finish(status_ptr);
        if (!status == UCS_OK) {
          // TODO: Handle error
          throw std::runtime_error("Error!");
        }
      }
    }
  }
}

std::unique_ptr<Message> Server::ReceiveMessage() {
  WaitUntilReadyToReceive();
  uint32_t message_size = 0;
  size_t received_length = 0;
  while (message_size == 0) {
    ucs_status_ptr_t status_ptr = endpoint_->receive(&message_size, sizeof(uint32_t), &received_length);
    ucs_status_t status = Finish(status_ptr);
    if (status != UCS_OK) {
      // TODO: Handle error
      throw std::runtime_error("Error!");
    }
  }
  std::unique_ptr<char> buffer((char *) malloc(message_size));
  ucs_status_ptr_t status_ptr = endpoint_->receive(buffer.get(), message_size, &received_length);
  ucs_status_t status = Finish(status_ptr);
  if (status != UCS_OK) {
    // TODO: Handle error
    throw ::std::runtime_error("Error!");
  }
  return std::make_unique<DetachedMessage>(std::move(buffer), message_size);
}

void Server::WaitUntilReadyToReceive() {
  ucp_stream_poll_ep_t *stream_poll_eps = (ucp_stream_poll_ep_t *) malloc(sizeof(ucp_stream_poll_ep_t) * 5);
  while (true) {
    ssize_t num_eps = ucp_stream_worker_poll(worker_.GetWorkerHandle(), stream_poll_eps, 5, 0);
    if (num_eps > 0) {
      if (stream_poll_eps->ep == endpoint_->GetHandle()) {
        break;
      }
    } else if (num_eps < 0) {
      throw std::runtime_error("Error!");
    } else {
      worker_.Progress();
    }
  }
  free(stream_poll_eps);
}

ucs_status_t Server::Finish(ucs_status_ptr_t status_ptr) {
  /*  if operation was completed immediately */
  if (status_ptr == nullptr) {
    return UCS_OK;
  }

  ucs_status_t status = ucp_request_check_status(status_ptr);
  while (status == UCS_INPROGRESS) {
    worker_.Progress();
    status = ucp_request_check_status(status_ptr);
  }
  ucp_request_free(status_ptr);
  return status;
}

void Server::CreateServerEndpoint(ucp_conn_request_h conn_request) {
  const ucp_ep_params_t params = CreateEndpointParams(conn_request);
  endpoint_ = std::make_unique<UCP::Endpoint>(worker_, &params);
}

void Server::InitializeConnection() {
  char init[] = "init";
  char recv[sizeof(init)] = "";
  size_t received_length;
  ucs_status_ptr_t status_ptr = endpoint_->receive(&recv, sizeof(init), &received_length);
  Finish(status_ptr);
  assert(recv == "init");
  status_ptr = endpoint_->send("init", sizeof(init));
  Finish(status_ptr);
}

void server_conn_req_cb(ucp_conn_request_h conn_request, void *arg) {
  Server *server = (Server *) arg;
  server->CreateServerEndpoint(conn_request);
//  server->InitializeConnection();
}
