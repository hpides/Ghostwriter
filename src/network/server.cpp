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
#include <deque>

Server::Server(std::unique_ptr<UCP::Worker> data_worker, std::unique_ptr<UCP::Worker> listening_worker, uint16_t port)
    : data_worker_(std::move(data_worker)),
      listening_worker_(std::move(listening_worker)) {
  StartListener(port);
}
void Server::StartListener(uint16_t port) {
  /* InitializeRequest the server's endpoint to NULL. Once the server's endpoint
* is created, this field will have a valid value. */
  ucs_status_t status;
  struct sockaddr_in6 listen_addr = CreateListenAddress(port);
  ucp_listener_params_t params = CreateListenerParams(&listen_addr);

  /* Create a listener on the server side to listen on the given address.*/
  status =
      ucp_listener_create(listening_worker_->GetWorkerHandle(), &params, &ucp_listener_);
  if (status != UCS_OK) {
    throw std::runtime_error(std::string("failed to create listener (%s)\n",
                                         ucs_status_string(status)));
  }
}

sockaddr_in6 Server::CreateListenAddress(uint16_t port) {
  sockaddr_in6 listen_addr;
  memset(&listen_addr, 0, sizeof(struct sockaddr_in6));
  listen_addr.sin6_family = AF_INET6;
  listen_addr.sin6_addr = in6addr_any;
  listen_addr.sin6_port = htons(port);
  return listen_addr;
}

ucp_listener_params_t Server::CreateListenerParams(sockaddr_in6 *listen_addr) {
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
      UCP_EP_PARAM_FIELD_ERR_HANDLER;
  params.err_handler.cb = err_cb;
  params.err_handler.arg = nullptr;
  params.conn_request = conn_request;
  return params;
}

void Server::Listen() {
  /* Server is always up */
  std::cout << "Listening for connection..." << std::endl;
  unsigned int progress;
  while (running_) {
    /* Wait for the server's callback to set the context->ep field, thus
     * indicating that the server's endpoint was created and is ready to
     * be used. The client side should initiate the connection, leading
     * to this ep's creation */
    progress = listening_worker_->Progress();
    if (running_ && !progress) {
      listening_worker_->Wait();
    }
  }
  std::cout <<"Stopped listening for connections." << std::endl;
}

void Server::Run(MessageHandler *message_handler) {
  if (running_) {
    throw std::runtime_error("Server is already running.");
  }
  running_ = true;
  message_handler_ = message_handler;
  listening_thread_ = std::move(std::thread(&Server::Listen, this));
  while (running_) {
    std::deque<UCP::Endpoint *> endpoints = WaitUntilReadyToReceive();
    if (running_) {
      for (UCP::Endpoint *endpoint : endpoints) {
        std::unique_ptr<Message> request = ReceiveMessage(*endpoint);
        std::unique_ptr<Message> response = message_handler_->HandleMessage(*request);
        if (!response->IsEmpty()) {
          ucs_status_ptr_t status_ptr = endpoint->send(response->GetBuffer(), response->GetSize());
          ucs_status_t status = Finish(status_ptr);
          if (status != UCS_OK) {
            throw std::runtime_error("Error!");
          }
        }
      }
    }
  }
}

void Server::Stop() {
  if (!running_) {
    throw std::runtime_error("Server is not running.");
  }
  running_ = false;
  listening_worker_->Signal();
  data_worker_->Signal();
  if (listening_thread_.joinable()) {
    listening_thread_.join();
  }
}

std::unique_ptr<Message> Server::ReceiveMessage(const UCP::Endpoint &endpoint) {
  uint32_t message_size = 0;
  size_t received_length = 0;
  while (message_size == 0) {
    ucs_status_ptr_t status_ptr = endpoint.receive(&message_size, sizeof(uint32_t), &received_length);
    ucs_status_t status = Finish(status_ptr);
    if (status != UCS_OK) {
      throw std::runtime_error("Error!");
    }
  }
  std::unique_ptr<char> buffer((char *) malloc(message_size));
  ucs_status_ptr_t status_ptr = endpoint.receive(buffer.get(), message_size, &received_length);
  ucs_status_t status = Finish(status_ptr);
  if (status != UCS_OK) {
    throw ::std::runtime_error("Error!");
  }
  return std::make_unique<DetachedMessage>(std::move(buffer), message_size);
}

std::deque<UCP::Endpoint *> Server::WaitUntilReadyToReceive() {
  std::deque<UCP::Endpoint *> result;
  while (true) {
    size_t num_of_eps = endpoint_map_.size();
    std::unique_ptr<ucp_stream_poll_ep_t[]> stream_poll_eps = std::make_unique<ucp_stream_poll_ep_t[]>(num_of_eps);
    ssize_t num_eps = ucp_stream_worker_poll(data_worker_->GetWorkerHandle(), stream_poll_eps.get(), num_of_eps, 0);
    if (num_eps > 0) {
      for (int i = 0; i < num_eps; i++) {
        result.push_back(endpoint_map_.at((stream_poll_eps.get() + i)->ep).get());
      }
      return result;
    } else if (num_eps < 0) {
      throw std::runtime_error("Error!");
    } else {
      data_worker_->Progress();
    }
  }
}

ucs_status_t Server::Finish(ucs_status_ptr_t status_ptr) {
  /*  if operation was completed immediately */
  if (status_ptr == nullptr) {
    return UCS_OK;
  }

  ucs_status_t status = ucp_request_check_status(status_ptr);
  while (status == UCS_INPROGRESS) {
    data_worker_->Progress();
    status = ucp_request_check_status(status_ptr);
  }
  ucp_request_free(status_ptr);
  return status;
}

void Server::CreateServerEndpoint(ucp_conn_request_h conn_request) {
  const ucp_ep_params_t params = CreateEndpointParams(conn_request);
  std::unique_ptr<UCP::Endpoint> endpoint = std::make_unique<UCP::Impl::Endpoint>(*data_worker_, &params);
  endpoint_map_[endpoint->GetHandle()] = std::move(endpoint);
}

void server_conn_req_cb(ucp_conn_request_h conn_request, void *arg) {
  Server *server = (Server *) arg;
  std::cout << "Create endpoint\n";
  server->CreateServerEndpoint(conn_request);
}
