#ifndef REMBRANDT_SRC_PRODUCER_SENDER_H_
#define REMBRANDT_SRC_PRODUCER_SENDER_H_

#include <thread>
#include <rembrandt/network/ucx/endpoint.h>
#include <rembrandt/network/connection_manager.h>
#include <rembrandt/network/request_processor.h>
#include "message_accumulator.h"
#include "producer_config.h"
#include <rembrandt/network/message.h>
#include <rembrandt/protocol/message_generator.h>
#include <rembrandt/network/client.h>

class Sender : public Client {
 public:
  Sender(std::unique_ptr<ConnectionManager> connection_manager_p,
         std::unique_ptr<MessageGenerator> message_generator_p,
         std::unique_ptr<RequestProcessor> request_processor_p,
         std::unique_ptr<UCP::Worker> worker_p,
         ProducerConfig config);
  ~Sender() = default;
  void Send(Batch *batch);
  void Send(Batch *batch, uint64_t (&latencies)[4]);
 private:
  ProducerConfig config_;
  UCP::Endpoint &GetEndpointWithRKey() const override;
  std::pair<uint64_t, uint64_t> Stage(Batch *batch);
  uint64_t message_size_;
  uint64_t logical_offset_;
  uint64_t remote_location_;
  uint64_t batch_;
  void Store(Batch *batch, uint64_t offset);
  bool Commit(Batch *batch, uint64_t at);
  bool Commit(uint32_t topic_id, uint32_t partition_id, uint64_t logical_offset, uint64_t message_size);
};

#endif //REMBRANDT_SRC_PRODUCER_SENDER_H_
