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
  Sender(ConnectionManager &connection_manager,
         MessageGenerator &message_generator,
         RequestProcessor &request_processor,
         UCP::Worker &worker,
         ProducerConfig &config);
  ~Sender() = default;
  void Send(Batch *batch);
 private:
  ProducerConfig &config_;
  UCP::Endpoint &GetEndpointWithRKey() const override;
  uint64_t Stage(Batch *batch);
  void Store(Batch *batch, uint64_t offset);
  bool Commit(Batch *batch, uint64_t offset);
  uint64_t ReceiveStagedOffset(const UCP::Endpoint &endpoint);
  bool ReceiveCommitResponse(const UCP::Endpoint &endpoint);
};

#endif //REMBRANDT_SRC_PRODUCER_SENDER_H_
