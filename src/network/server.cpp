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
    : data_worker_(std::move(data_worker)), lsock(-1) {
  StartListener(port);
}
void Server::StartListener(uint16_t port) {
  /* InitializeRequest the server's endpoint to NULL. Once the server's endpoint
* is created, this field will have a valid value. */
  ucs_status_t status;
  struct sockaddr_in listen_addr = CreateListenAddress(port);
  lsock = socket(AF_INET, SOCK_STREAM, 0);
  if (lsock < 0) {
    throw std::runtime_error("error: open server socket");
  }
  int optval = 1;
  auto ret = setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
  if (ret < 0) {
    throw std::runtime_error("error: server ssetockopt");
  }
  ret = bind(lsock, (struct sockaddr*)&listen_addr, sizeof(listen_addr));
  if (ret < 0) {
    throw std::runtime_error("error: bind server");
  }
  ret = listen(lsock, 0);
  if (ret < 0) {
    throw std::runtime_error("error: listen server");
  }

  /* Create a listener on the server side to listen on the given address.*/
}

sockaddr_in Server::CreateListenAddress(uint16_t port) {
  sockaddr_in listen_addr;
  memset(&listen_addr, 0, sizeof(struct sockaddr_in));
  listen_addr.sin_family = AF_INET;
  listen_addr.sin_addr.s_addr = INADDR_ANY;
  listen_addr.sin_port = htons(port);
  return listen_addr;
}

/**
 * Error handling callback.
 */
static void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status) {
  printf("error handling callback was invoked with status %d (%s)\n",
         status, ucs_status_string(status));
}

ucp_ep_params_t Server::CreateEndpointParams(ucp_address_t *addr) {
  ucp_ep_params_t params;
  memset(&params, 0, sizeof(ucp_ep_params_t));
  params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS |
      UCP_EP_PARAM_FIELD_ERR_HANDLER;
  params.address = addr;
  params.err_handler.cb = err_cb;
  params.err_handler.arg = nullptr;
  return params;
}

void Server::Listen() {
  /* Server is always up */
  std::cout << "Listening for connection..." << std::endl;

  int dsock;
  while (running_) {
    dsock = -1;
    dsock = accept(lsock, NULL, NULL);
    if (dsock < 0) {
      throw std::runtime_error("error: accept server connection");
    }
    size_t client_addr_len;
    auto ret = recv(dsock, &client_addr_len, sizeof(client_addr_len), MSG_WAITALL);
    if (ret != (int) sizeof(client_addr_len)) {
      throw std::runtime_error("recv client address length");
    }

    // FIXME: Free memory
    ucp_address_t *client_addr_p = (ucp_address_t *) malloc(client_addr_len);
    ret = recv(dsock, client_addr_p, client_addr_len, MSG_WAITALL);
    if (ret != (int) client_addr_len) {
      throw std::runtime_error("recv client address");
    }
 
    ucp_address_t *addr;
    size_t addr_len;
    auto status = ucp_worker_get_address(data_worker_->GetWorkerHandle(), &addr, &addr_len);
    ret = send(dsock, &addr_len, sizeof(addr_len), 0);
    if (ret != (int) sizeof(addr_len)) {
      throw std::runtime_error("send client address length");
    }
    ret = send(dsock, &addr, addr_len, 0);
    if (ret != (int) addr_len) {
      throw std::runtime_error("send client address");
    }
    close(dsock);
    CreateServerEndpoint(client_addr_p);
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

void Server::CreateServerEndpoint(ucp_address_t *addr) {
  const ucp_ep_params_t params = CreateEndpointParams(addr);
  std::unique_ptr<UCP::Endpoint> endpoint = std::make_unique<UCP::Impl::Endpoint>(*data_worker_, &params);
  endpoint_map_[endpoint->GetHandle()] = std::move(endpoint);
}
