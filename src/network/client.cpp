#include "rembrandt/network/client.h"

Client::Client(ConnectionManager &connection_manager,
               MessageGenerator &message_generator,
               RequestProcessor &request_processor,
               UCP::Worker &worker) : connection_manager_(connection_manager),
                                      message_generator_(message_generator),
                                      request_processor_(request_processor),
                                      worker_(worker) {}

Client::~Client() noexcept {}

UCP::Endpoint &Client::GetEndpointWithRKey(const std::string &server_addr, uint16_t port) const {
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(server_addr, port);
  if (!endpoint.hasRKey()) {
    connection_manager_.RegisterRemoteMemory(server_addr, port);
  }
  return endpoint;
}

void Client::SendMessage(const Message &message, const UCP::Endpoint &endpoint) {
  ucs_status_ptr_t ucs_status_ptr = endpoint.send(message.GetBuffer(), message.GetSize());
  ucs_status_t status = request_processor_.Process(ucs_status_ptr);
  if (status != UCS_OK) {
    throw std::runtime_error("Failed sending request!\n");
  }
}

std::unique_ptr<char> Client::ReceiveMessage(const UCP::Endpoint &endpoint) {
  uint32_t message_size;
  size_t received_length;
  ucs_status_ptr_t status_ptr = endpoint.receive(&message_size, sizeof(uint32_t), &received_length);
  ucs_status_t status = request_processor_.Process(status_ptr);
  if (!status == UCS_OK) {
    throw std::runtime_error("Error!");
  }
  std::unique_ptr<char> buffer((char *) malloc(message_size));
  status_ptr = endpoint.receive(buffer.get(), message_size, &received_length);
  status = request_processor_.Process(status_ptr);
  if (!status == UCS_OK) {
    throw ::std::runtime_error("Error!");
  }
  return buffer;
}

void Client::WaitUntilReadyToReceive(const UCP::Endpoint &endpoint) {
  ucp_stream_poll_ep_t *stream_poll_eps = (ucp_stream_poll_ep_t *) malloc(sizeof(ucp_stream_poll_ep_t) * 5);
  while (true) {
    ssize_t num_eps = ucp_stream_worker_poll(worker_.GetWorkerHandle(), stream_poll_eps, 5, 0);
    if (num_eps > 0) {
      if (stream_poll_eps->ep == endpoint.GetHandle()) {
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
