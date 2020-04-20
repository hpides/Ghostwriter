#include <rembrandt/network/utils.h>
#include <iostream>
#include <rembrandt/network/message.h>
#include "rembrandt/consumer/consumer.h"

Consumer::Consumer(Receiver &receiver, ConsumerConfig &config) : receiver_(receiver), config_(config) {}

void *Consumer::Receive(TopicPartition topic_partition,
                        uint64_t offset,
                        size_t max_length) {
  void *buffer = malloc(max_length);
  // TODO: Use concrete endpoints
  flatbuffers::FlatBufferBuilder builder(129);
  auto fetch = Rembrandt::Protocol::CreateFetch(
      builder,
      topic_partition.first,
      topic_partition.second,
      offset,
      max_length);
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder,
      message_counter_,
      Rembrandt::Protocol::Message_Fetch,
      fetch.Union());
  message_counter_++;
  builder.FinishSizePrefixed(message);
  const flatbuffers::DetachedBuffer detached_buffer = builder.Release();
  Message fetch_message = Message(std::unique_ptr<char>((char *) detached_buffer.data()), detached_buffer.size());

  UCP::Endpoint &endpoint = client_GetConnection("", 1);
//  UCP::Endpoint endpoint = client_.GetConnection(config_.broker_node_ip, config_.broker_node_port);
  ucs_status_ptr_t ucs_status_ptr = endpoint.send(fetch_message.GetBuffer(), fetch_message.GetSize());
  ucs_status_t status = client_.ProcessRequest(ucs_status_ptr);
  if (status != UCS_OK) {
    throw std::runtime_error("Failed sending stage request!\n");
  }
  Rembrandt::Protocol::Fetched *fetched = HandleFetchResponse()

  ucs_status_ptr_t status_ptr = ep.get(buffer, length, offset, print_cb);

  if (status_ptr == NULL) {
    std::cout << "Request completed immediately\n";
    return buffer;
  }

  if (UCS_PTR_IS_ERR(status_ptr)) {
    throw std::runtime_error("Runtime error!\n");
  }
  ucs_status_t status;
  do {
    ucp_worker_progress(client_.GetWorker().GetWorkerHandle());
    status = ucp_request_check_status(status_ptr);
  } while (status == UCS_INPROGRESS);

  /* This request may be reused so initialize it for next time */
  ucp_request_free(status_ptr);

  // TODO: Handle errors
  if (status != UCS_OK) {
    throw std::runtime_error("Failed receiving\n");
  }
  std::cout << "Done receiving\n";

  return buffer;
}

const Rembrandt::Protocol::Fetched *Consumer::HandleFetchResponse(Message &raw_message) {
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(raw_message.GetBuffer());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_Fetched: {
      return static_cast<const Rembrandt::Protocol::Fetched *> (base_message->content());
    }
    case Rembrandt::Protocol::Message_FetchFailed: {
      throw std::runtime_error("Not implemented!");
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}

