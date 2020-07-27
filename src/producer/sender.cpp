#include <iostream>
#include <stdexcept>
#include <rembrandt/network/request_processor.h>
#include <rembrandt/network/detached_message.h>
#include "../../include/rembrandt/network/utils.h"
#include "../../include/rembrandt/network/ucx/endpoint.h"
#include "../../include/rembrandt/producer/sender.h"
#include <chrono>

Sender::Sender(ConnectionManager &connection_manager,
               MessageGenerator &message_generator,
               RequestProcessor &request_processor,
               UCP::Worker &worker,
               ProducerConfig &config) : Client(connection_manager,
                                                message_generator,
                                                request_processor,
                                                worker),
                                         config_(config),
                                         message_size_(0),
                                         logical_offset_(0),
                                         remote_location_(0),
                                         batch_(0) {}

void Sender::Send(Batch *batch) {
  auto[logical_offset, remote_location] = Stage(batch);
  Store(batch, remote_location);
  Commit(batch, logical_offset);
}

void Sender::Send(Batch *batch, uint64_t (&latencies)[4]) {
  auto start = std::chrono::steady_clock::now();
  auto[logical_offset, remote_location] = Stage(batch);
  auto staged = std::chrono::steady_clock::now();
  Store(batch, remote_location);
  auto stored = std::chrono::steady_clock::now();
  Commit(batch, logical_offset);
  auto committed = std::chrono::steady_clock::now();
  long event_before = *(long *) batch->getBuffer();
  latencies[0] = std::chrono::duration_cast<std::chrono::microseconds>(start.time_since_epoch()).count() - event_before;
  latencies[1] = std::chrono::duration_cast<std::chrono::microseconds>(staged - start).count();
  latencies[2] = std::chrono::duration_cast<std::chrono::microseconds>(stored - staged).count();
  latencies[3] = std::chrono::duration_cast<std::chrono::microseconds>(committed - stored).count();
}

void Sender::Store(Batch *batch, uint64_t offset) {
  UCP::Endpoint &endpoint = GetEndpointWithRKey();

  ucs_status_ptr_t status_ptr = endpoint.put(batch->getBuffer(),
                                             batch->getSize(),
                                             endpoint.GetRemoteAddress() + offset,
                                             empty_cb);
  ucs_status_ptr_t flush_ptr = endpoint.flush(empty_cb);
  ucs_status_t status = request_processor_.Process(status_ptr);
  ucs_status_t flush = request_processor_.Process(flush_ptr);

  if (flush != UCS_OK || status != UCS_OK) {
    throw std::runtime_error("Failed storing batch!\n");
  }
}

UCP::Endpoint &Sender::GetEndpointWithRKey() const {
  return Client::GetEndpointWithRKey(config_.storage_node_ip,
                                     config_.storage_node_port);
}

std::pair<uint64_t, uint64_t> Sender::Stage(Batch *batch) {
  if (batch_ < 1) {
    std::unique_ptr<Message> stage_message =
        message_generator_.StageRequest(batch->getTopic(), batch->getPartition(), batch->getSize(), 1);
    UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.broker_node_ip, config_.broker_node_port);
    SendMessage(*stage_message, endpoint);
    std::unique_ptr<char> buffer = Client::ReceiveMessage(endpoint);
    auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(buffer.get());
    auto union_type = base_message->content_type();
    switch (union_type) {
      case Rembrandt::Protocol::Message_StageResponse: {
        auto staged = static_cast<const Rembrandt::Protocol::StageResponse *> (base_message->content());
        logical_offset_ = staged->logical_offset();
        remote_location_ = staged->remote_location();
        message_size_ = staged->effective_message_size();
        batch_ = staged->batch();
        if (batch_ < 1) {
          throw std::runtime_error("Batch size less than 1!");
        }
        break;
      }
      case Rembrandt::Protocol::Message_StageException: {
        throw std::runtime_error("Handling StageException not implemented!");
      }
      default: {
        throw std::runtime_error("Message type not available!");
      }
    }
  }
  uint64_t logical_offset = logical_offset_;
  uint64_t remote_location = remote_location_;
  logical_offset_ += message_size_;
  remote_location_ += message_size_;
  --batch_;
  return std::pair<uint64_t, uint64_t>(logical_offset, remote_location);
}

bool Sender::Commit(Batch *batch, uint64_t at) {
  return Commit(batch->getTopic(), batch->getPartition(), at, message_size_);
}

bool Sender::Commit(uint32_t topic_id, uint32_t partition_id, uint64_t logical_offset, uint64_t message_size) {
  std::unique_ptr<Message> commit_message = message_generator_.CommitRequest(topic_id,
                                                                             partition_id,
                                                                             logical_offset,
                                                                             message_size);
  UCP::Endpoint &endpoint = connection_manager_.GetConnection(config_.broker_node_ip,
                                                              config_.broker_node_port);
  SendMessage(*commit_message, endpoint);
  std::unique_ptr<char> buffer = Client::ReceiveMessage(endpoint);
  auto base_message = flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(buffer.get());
  auto union_type = base_message->content_type();
  switch (union_type) {
    case Rembrandt::Protocol::Message_CommitResponse: {
      return true;
    }
    case Rembrandt::Protocol::Message_CommitException: {
      throw std::runtime_error("CommitRequest failed.");
    }
    default: {
      throw std::runtime_error("Message type not available!");
    }
  }
}
