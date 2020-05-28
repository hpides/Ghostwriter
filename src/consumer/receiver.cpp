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

std::unique_ptr<ConsumerSegmentInfo> Receiver::FetchSegmentInfo(uint32_t topic_id,
                                                                uint32_t partition_id,
                                                                uint32_t segment_id) {
  std::unique_ptr<ConsumerSegmentInfo>
      result = std::make_unique<ConsumerSegmentInfo>(topic_id, partition_id, segment_id);
  UpdateSegmentInfo(*result);
  return result;
}

void Receiver::UpdateSegmentInfo(ConsumerSegmentInfo &consumer_segment_info) {
  std::unique_ptr<Message> fetch_request = message_generator_.FetchRequest(consumer_segment_info.topic_id,
                                                                           consumer_segment_info.partition_id,
                                                                           consumer_segment_info.segment_id);
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.broker_node_ip, config_.broker_node_port);
  SendMessage(*fetch_request, endpoint);
  ReceiveFetched(endpoint, consumer_segment_info);
}

FetchedData Receiver::ReceiveFetchedData(UCP::Endpoint &endpoint) {
  std::unique_ptr<char> buffer = Client::ReceiveMessage(endpoint);
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(buffer.get());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_FetchResponse: {
      auto fetch_response = static_cast<const Rembrandt::Protocol::FetchResponse *> (base_message->content());
      return FetchedData{fetch_response->start_offset(), fetch_response->commit_offset(), fetch_response->is_committable()};
    }
    case Rembrandt::Protocol::Message_FetchException: {
      throw std::runtime_error("Handling FetchException not implemented!");
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}

void Receiver::ReceiveFetched(UCP::Endpoint &endpoint, ConsumerSegmentInfo &consumer_segment_info) {
  FetchedData fetched_data = ReceiveFetchedData(endpoint);
  consumer_segment_info.read_offset = std::max(consumer_segment_info.read_offset, fetched_data.start_offset);
  consumer_segment_info.commit_offset = fetched_data.commit_offset;
  consumer_segment_info.is_committable = fetched_data.is_committable;
}
