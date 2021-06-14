#include "rembrandt/network/client.h"

Client::Client::Client(std::unique_ptr<ConnectionManager> connection_manager_p,
                       std::unique_ptr<MessageGenerator> message_generator_p,
                       std::unique_ptr<RequestProcessor> request_processor_p,
                       std::unique_ptr<UCP::Worker> worker_p)
    : connection_manager_p_(std::move(connection_manager_p)),
      message_generator_p_(std::move(message_generator_p)),
      request_processor_p_(std::move(request_processor_p)),
      worker_p_(std::move(worker_p)) {}

Client::~Client() noexcept {}

UCP::Endpoint &Client::GetEndpointWithRKey(const std::string &server_addr,
                                           uint16_t port) const {
  UCP::Endpoint &endpoint =
      connection_manager_p_->GetConnection(server_addr, port);
  if (!endpoint.hasRKey()) {
    connection_manager_p_->RegisterRemoteMemory(server_addr, port);
  }
  return endpoint;
}

void Client::SendMessage(const Message &message,
                         const UCP::Endpoint &endpoint) {
  ucs_status_ptr_t ucs_status_ptr =
      endpoint.send(message.GetBuffer(), message.GetSize());
  ucs_status_t status = request_processor_p_->Process(ucs_status_ptr);
  if (status != UCS_OK) {
    throw std::runtime_error("Failed sending request!\n");
  }
}

std::unique_ptr<char> Client::ReceiveMessage(const UCP::Endpoint &endpoint) {
  uint32_t message_size;
  size_t received_length;
  ucs_status_ptr_t status_ptr =
      endpoint.receive(&message_size, sizeof(uint32_t), &received_length);
  ucs_status_t status = request_processor_p_->Process(status_ptr);
  if (!status == UCS_OK) {
    throw std::runtime_error("Error!");
  }
  std::unique_ptr<char> buffer((char *)malloc(message_size));
  status_ptr = endpoint.receive(buffer.get(), message_size, &received_length);
  status = request_processor_p_->Process(status_ptr);
  if (!status == UCS_OK) {
    throw ::std::runtime_error("Error!");
  }
  return buffer;
}

void Client::WaitUntilReadyToReceive(const UCP::Endpoint &endpoint) {
  ucp_stream_poll_ep_t *stream_poll_eps =
      (ucp_stream_poll_ep_t *)malloc(sizeof(ucp_stream_poll_ep_t) * 5);
  while (true) {
    ssize_t num_eps = ucp_stream_worker_poll(worker_p_->GetWorkerHandle(),
                                             stream_poll_eps, 5, 0);
    if (num_eps > 0) {
      if (stream_poll_eps->ep == endpoint.GetHandle()) {
        break;
      }
    } else if (num_eps < 0) {
      throw std::runtime_error("Error!");
    } else {
      worker_p_->Progress();
    }
  }
  free(stream_poll_eps);
}
