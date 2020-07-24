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

std::unique_ptr<Message> Receiver::Receive(std::unique_ptr<Message> message, uint64_t offset) {
  UCP::Endpoint &storage_endpoint = GetEndpointWithRKey();
  ucs_status_ptr_t status_ptr = storage_endpoint.get(message->GetBuffer(),
                                                     message->GetSize(),
                                                     storage_endpoint.GetRemoteAddress() + offset,
                                                     empty_cb);
  ucs_status_t status = request_processor_.Process(status_ptr);

  if (status != UCS_OK) {
    throw std::runtime_error("Failed fetching data\n");
  }
  return message;
}

UCP::Endpoint &Receiver::GetEndpointWithRKey() const {
  return Client::GetEndpointWithRKey(config_.storage_node_ip,
                                     config_.storage_node_port);
}

std::unique_ptr<ReadSegment> Receiver::Fetch(uint32_t topic_id, uint32_t partition_id, uint64_t logical_offset) {
  std::unique_ptr<Message> read_segment_request = message_generator_.FetchRequest(topic_id,
                                                                                  partition_id,
                                                                                  logical_offset);
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.broker_node_ip, config_.broker_node_port);
  SendMessage(*read_segment_request, endpoint);
  std::unique_ptr<char> buffer = Client::ReceiveMessage(endpoint);
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(buffer.get());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_FetchResponse: {
      auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(buffer.get());
      auto fetch_response = static_cast<const Rembrandt::Protocol::FetchResponse *> (base_message->content());
      return std::make_unique<ReadSegment>(topic_id,
                                           partition_id,
                                           logical_offset,
                                           fetch_response->commit_offset(),
                                           fetch_response->remote_location());
    }
    case Rembrandt::Protocol::Message_FetchException: {
      throw std::runtime_error("Handling FetchException not implemented!");
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}
