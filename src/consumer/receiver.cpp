#include <rembrandt/network/utils.h>
#include "rembrandt/consumer/receiver.h"

Receiver::Receiver(ConnectionManager &connection_manager,
                   MessageGenerator &message_generator,
                   RequestProcessor &request_processor,
                   UCP::Worker &worker,
                   ConsumerConfig &config) : Client(connection_manager,
                                                    message_generator,
                                                    request_processor,
                                                    worker),
                                             config_(config) {}

std::unique_ptr<Message> Receiver::Receive(TopicPartition topic_partition,
                                           std::unique_ptr<Message> message,
                                           uint64_t offset) {
  std::unique_ptr<Message> fetch_message = message_generator_.Fetch(topic_partition, offset, message->GetSize());
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.broker_node_ip,
                                                              config_.broker_node_port);
  SendMessage(*fetch_message, endpoint);
  std::pair<uint64_t, uint32_t> data_location = ReceiveFetchedDataLocation(endpoint);
  UCP::Endpoint &storage_endpoint = GetEndpointWithRKey();
  ucs_status_ptr_t status_ptr = storage_endpoint.get(message->GetBuffer(),
                                                     data_location.second,
                                                     storage_endpoint.GetRemoteAddress() + data_location.first,
                                                     empty_cb);
  ucs_status_t status = request_processor_.Process(status_ptr);

  if (status != UCS_OK) {
    throw std::runtime_error("Failed fetching data\n");
  }
  return std::move(message);
}

UCP::Endpoint &Receiver::GetEndpointWithRKey() const {
  return Client::GetEndpointWithRKey(config_.storage_node_ip,
                                     config_.storage_node_port,
                                     config_.storage_node_rkey_port);
}

std::pair<uint64_t, uint32_t> Receiver::ReceiveFetchedDataLocation(UCP::Endpoint &endpoint) {
  std::unique_ptr<char> buffer = Client::ReceiveMessage(endpoint);
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(buffer.get());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_Fetched: {
      auto fetched = static_cast<const Rembrandt::Protocol::Fetched *> (base_message->content());
      return std::pair(fetched->offset(), fetched->length());
    }
    case Rembrandt::Protocol::Message_FetchFailed: {
      throw std::runtime_error("Not implemented!");
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}
