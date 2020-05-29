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
  return std::move(message);
}

UCP::Endpoint &Receiver::GetEndpointWithRKey() const {
  return Client::GetEndpointWithRKey(config_.storage_node_ip,
                                     config_.storage_node_port);
}

std::unique_ptr<ReadSegment> Receiver::FetchReadSegment(uint32_t topic_id,
                                                        uint32_t partition_id,
                                                        uint32_t segment_id) {
  std::unique_ptr<char> response_buffer = RequestReadSegment(topic_id,
                                                             partition_id,
                                                             segment_id);
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(response_buffer.get());
  auto read_segment_response = static_cast<const Rembrandt::Protocol::ReadSegmentResponse *> (base_message->content());
  return std::make_unique<ReadSegment>(read_segment_response->topic_id(),
                                       read_segment_response->partition_id(),
                                       read_segment_response->segment_id(),
                                       read_segment_response->start_offset(),
                                       read_segment_response->commit_offset(),
                                       read_segment_response->is_committable());
}

void Receiver::UpdateReadSegment(ReadSegment &read_segment) {
  std::unique_ptr<char> response_buffer = RequestReadSegment(read_segment.topic_id,
                                                              read_segment.partition_id,
                                                              read_segment.segment_id);
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(response_buffer.get());
  auto read_segment_response = static_cast<const Rembrandt::Protocol::ReadSegmentResponse *> (base_message->content());
  read_segment.commit_offset = read_segment_response->commit_offset();
  read_segment.is_committable = read_segment_response->is_committable();
}

std::unique_ptr<char> Receiver::RequestReadSegment(uint32_t topic_id,
                                                   uint32_t partition_id,
                                                   uint64_t segment_id) {
  std::unique_ptr<Message> read_segment_request = message_generator_.ReadSegmentRequest(topic_id,
                                                                                        partition_id,
                                                                                        segment_id,
                                                                                        false);
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.broker_node_ip, config_.broker_node_port);
  SendMessage(*read_segment_request, endpoint);
  std::unique_ptr<char> buffer = Client::ReceiveMessage(endpoint);
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(buffer.get());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_ReadSegmentResponse: {
      return std::move(buffer);
    }
    case Rembrandt::Protocol::Message_ReadSegmentException: {
      throw std::runtime_error("Handling ReadSegmentException not implemented!");
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}
