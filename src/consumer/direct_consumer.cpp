#include "rembrandt/consumer/direct_consumer.h"
#include <rembrandt/broker/broker_node.h>

DirectConsumer::DirectConsumer(std::unique_ptr<Receiver> receiver_p, ConsumerConfig config) : receiver_p_(std::move(
    receiver_p)), read_segment_(nullptr), config_(config) {}

std::unique_ptr<DirectConsumer> DirectConsumer::Create(ConsumerConfig config, UCP::Context &context) {
  std::unique_ptr<MessageGenerator> message_generator_p;
  std::unique_ptr<UCP::EndpointFactory> endpoint_factory_p;
  std::unique_ptr<UCP::Worker> worker_p = context.CreateWorker();
  std::unique_ptr<RequestProcessor> request_processor_p = std::make_unique<RequestProcessor>(*worker_p);
  std::unique_ptr<ConnectionManager>
      connection_manager_p = std::make_unique<ConnectionManager>(std::move(endpoint_factory_p),
                                                                 *worker_p,
                                                                 *message_generator_p,
                                                                 *request_processor_p);
  std::unique_ptr<Receiver> receiver_p = std::make_unique<Receiver>(std::move(connection_manager_p),
                                                                    std::move(message_generator_p),
                                                                    std::move(request_processor_p),
                                                                    std::move(worker_p),
                                                                    config);
  return std::unique_ptr<DirectConsumer>(new DirectConsumer(std::move(receiver_p), config));
}

std::unique_ptr<Message> DirectConsumer::Receive(uint32_t topic_id,
                                                 uint32_t partition_id,
                                                 std::unique_ptr<Message> message) {
  switch (config_.mode) {
    case Partition::Mode::EXCLUSIVE:return ExclusiveReceive(topic_id, partition_id, std::move(message));
    case Partition::Mode::CONCURRENT:return ConcurrentReceive(topic_id, partition_id, std::move(message));
    default:throw std::runtime_error("Mode not handled");
  }
}

std::unique_ptr<Message> DirectConsumer::ExclusiveReceive(uint32_t topic_id,
                                                          uint32_t partition_id,
                                                          std::unique_ptr<Message> message) {
  uint64_t offset = AdvanceReadOffset(topic_id, partition_id, message->GetSize());
  return receiver_p_->Receive(std::move(message), offset);
}

std::unique_ptr<Message> DirectConsumer::ConcurrentReceive(uint32_t topic_id,
                                                           uint32_t partition_id,
                                                           std::unique_ptr<Message> message) {
  uint64_t *flag;
  do {
    message = ExclusiveReceive(topic_id, partition_id, std::move(message));
    flag = (uint64_t *) (message->GetBuffer() + message->GetSize() - sizeof(BrokerNode::COMMIT_FLAG));
  } while (*flag == BrokerNode::TIMEOUT_FLAG);
  if (*flag != BrokerNode::COMMIT_FLAG) {
    throw std::runtime_error("Unknown flag value");
  }
  return message;
}

uint64_t DirectConsumer::AdvanceReadOffset(uint32_t topic_id, uint32_t partition_id, uint64_t message_size) {
  uint64_t offset;
  do {
    offset = GetNextOffset(topic_id, partition_id);
  } while (read_segment_->logical_offset + message_size > read_segment_->commit_offset);
  read_segment_->remote_location += message_size;
  read_segment_->logical_offset += message_size;
  return offset;
}

uint64_t DirectConsumer::GetNextOffset(uint32_t topic_id, uint32_t partition_id) {
  if (read_segment_ == nullptr) {
    read_segment_ = receiver_p_->Fetch(topic_id, partition_id, 0);
  } else if (read_segment_->logical_offset == read_segment_->commit_offset) {
    read_segment_ = receiver_p_->Fetch(topic_id, partition_id, read_segment_->logical_offset);
  }
  return read_segment_->remote_location;
}
