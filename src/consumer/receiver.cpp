#include <rembrandt/network/utils.h>
#include "rembrandt/consumer/receiver.h"

Receiver::Receiver(ConnectionManager &connection_manager,
                   MessageGenerator &message_generator,
                   RequestProcessor &request_processor,
                   ConsumerConfig &config) : connection_manager_(connection_manager),
                                             message_generator_(message_generator),
                                             request_processor_(request_processor),
                                             config_(config) {}

void *Receiver::Next(TopicPartition topic_partition) {
  uint64_t offset;
  auto it = read_offsets_.find(topic_partition);
  if (it == read_offsets_.end()) {
    offset = 0;
    read_offsets_[topic_partition] = 0;
  } else {
    offset = it->second;
  }
  std::unique_ptr<Message> fetch_message = message_generator_.Fetch(topic_partition, offset, config_.max_batch_size);
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.broker_node_ip, config_.broker_node_port);
  SendMessage(*fetch_message, endpoint);
  std::pair<uint64_t, uint32_t> data_location = ReceiveFetchedDataLocation(endpoint);
  UCP::Endpoint &storage_endpoint = GetEndpointWithRKey();
  void *buffer = malloc(data_location.second);
  ucs_status_ptr_t status_ptr = storage_endpoint.get(buffer,
                                                     data_location.second,
                                                     storage_endpoint.GetRemoteAddress() + data_location.first,
                                                     empty_cb);
  ucs_status_t status = request_processor_.Process(status_ptr);

  if (status != UCS_OK) {
    throw std::runtime_error("Failed fetching data\n");
  }
  return buffer;
}

UCP::Endpoint &Receiver::GetEndpointWithRKey() const {
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.storage_node_ip, config_.storage_node_port);
  if (!endpoint.hasRKey()) {
    connection_manager_.RegisterRemoteMemory(config_.storage_node_ip,
                                             config_.storage_node_port,
                                             config_.storage_node_rkey_port);
  }
  return endpoint;
}
std::pair<uint64_t, uint32_t> Receiver::ReceiveFetchedDataLocation(UCP::Endpoint &endpoint) {
  uint32_t message_size;
  size_t received_length;
  ucs_status_ptr_t status_ptr = endpoint.receive(&message_size, sizeof(uint32_t), &received_length);
  ucs_status_t status = request_processor_.Process(status_ptr);
  if (!status == UCS_OK) {
    // TODO: Handle error
    throw std::runtime_error("Error!");
  }
  std::unique_ptr<char> buffer((char *) malloc(message_size));
  status_ptr = endpoint.receive(buffer.get(), message_size, &received_length);
  status = request_processor_.Process(status_ptr);
  if (!status == UCS_OK) {
    // TODO: Handle error
    throw ::std::runtime_error("Error!");
  }
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
void Receiver::SendMessage(Message &message, UCP::Endpoint &endpoint) {
  ucs_status_ptr_t ucs_status_ptr = endpoint.send(message.GetBuffer(), message.GetSize());
  ucs_status_t status = request_processor_.Process(ucs_status_ptr);
  if (status != UCS_OK) {
    throw std::runtime_error("Failed sending stage request!\n");
  }
  // TODO: Adjust to handling different response types
}
