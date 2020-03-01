#include <functional>
#include <iostream>
#include <stdexcept>
#include "../../include/rembrandt/network/utils.h"
#include "../../include/rembrandt/protocol/rembrandt_protocol_generated.h"
#include "../../include/rembrandt/network/ucx/endpoint.h"
#include "../../include/rembrandt/network/message.h"
#include "../../include/rembrandt/producer/sender.h"
#include "../../include/rembrandt/producer/message_accumulator.h"

Sender::Sender(UCP::Client &client, MessageAccumulator &message_accumulator, ProducerConfig &config)
    : config_(config),
      client_(client),
      message_accumulator_(message_accumulator) {}

void Sender::Start(UCP::Endpoint &ep) {
  if (!running) {
    running = true;
    thread_ = std::thread(&Sender::Run, this, std::ref(ep));
  } else {
    std::cout << "Sender already running.\n";
  }
}

void Sender::Stop() {
  if (running) {
    running = false;
    thread_.join();
  } else {
    std::cout << "Sender not running.\n";
  }
}

void Sender::Run(UCP::Endpoint &ep) {
  while (running) {
    Batch *batch = message_accumulator_.GetFullBatch();
    Send(batch);
  }
}

void Sender::Send(Batch *batch) {
  uint64_t offset = Stage(batch);
  Store(batch, offset);
  // TODO: Check success
//  Commit(offset);
};

void Sender::Store(Batch *batch, uint64_t offset) {
  UCP::Endpoint &endpoint = client_.GetConnection(config_.storage_node_ip, config_.storage_node_port);
  ucs_status_ptr_t status_ptr = endpoint.put(batch->getBuffer(),
                                             batch->getSize(),
                                             endpoint.GetRemoteAddress() + offset,
                                             empty_cb);
  ucs_status_t status = client_.ProcessRequest(status_ptr);
  message_accumulator_.Free(batch);

  if (status != UCS_OK) {
    throw std::runtime_error("Failed storing batch!\n");
  }
}

uint64_t Sender::Stage(Batch *batch) {
  flatbuffers::FlatBufferBuilder builder(128);
  auto stage = Rembrandt::Protocol::CreateStage(
      builder,
      batch->getTopic(),
      batch->getPartition(),
      batch->getNumMessages(),
      batch->getSize());
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder,
      message_counter_,
      Rembrandt::Protocol::Message_Stage,
      stage.Union());
  message_counter_++;
  builder.FinishSizePrefixed(message);
  const flatbuffers::DetachedBuffer detached_buffer = builder.Release();
  Message stage_message = Message(std::unique_ptr<char>((char *) detached_buffer.data()), detached_buffer.size());

  UCP::Endpoint &endpoint = client_.GetConnection(config_.broker_node_ip, config_.broker_node_port);
  ucs_status_ptr_t ucs_status_ptr = endpoint.send(stage_message.GetBuffer(), stage_message.GetSize());
  ucs_status_t status = client_.ProcessRequest(ucs_status_ptr);
  if (status != UCS_OK) {
    throw std::runtime_error("Failed sending stage request!\n");
  }
  // TODO: Adjust to handling different response types
  uint64_t offset;
  size_t received_length;
  ucs_status_ptr = endpoint.receive(&offset, sizeof(offset), &received_length);
  status = client_.ProcessRequest(ucs_status_ptr);
  if (status != UCS_OK) {
    throw std::runtime_error("Failed receiving stage response!\n");
  }
  return offset;
}