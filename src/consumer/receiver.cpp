#include <rembrandt/network/utils.h>
#include "rembrandt/consumer/receiver.h"

Receiver::Receiver(std::unique_ptr<ConnectionManager> connection_manager_p,
                   std::unique_ptr<MessageGenerator> message_generator_p,
                   std::unique_ptr<RequestProcessor> request_processor_p,
                   std::unique_ptr<UCP::Worker> worker_p,
                   ConsumerConfig config) : Client(std::move(connection_manager_p),
                                                    std::move(message_generator_p),
                                                    std::move(request_processor_p),
                                                    std::move(worker_p)),
                                             config_(config) {}

std::unique_ptr<Message> Receiver::Receive(std::unique_ptr<Message> message, uint64_t offset) {
  UCP::Endpoint &storage_endpoint = GetEndpointWithRKey();
  ucs_status_ptr_t status_ptr = storage_endpoint.get(message->GetBuffer(),
                                                     message->GetSize(),
                                                     storage_endpoint.GetRemoteAddress() + offset,
                                                     empty_cb);
  ucs_status_t status = request_processor_p_->Process(status_ptr);

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
  std::unique_ptr<Message> read_segment_request = message_generator_p_->FetchRequest(topic_id,
                                                                                  partition_id,
                                                                                  logical_offset);
  Rembrandt::Protocol::Message union_type;
  std::unique_ptr<char> buffer = nullptr;
  do {
    UCP::Endpoint &endpoint = connection_manager_p_->GetConnection(config_.broker_node_ip, config_.broker_node_port);
    SendMessage(*read_segment_request, endpoint);
    buffer = Client::ReceiveMessage(endpoint);
    auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(buffer.get());
    union_type = base_message->content_type();
  } while (union_type == Rembrandt::Protocol::Message_FetchException);
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
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}
