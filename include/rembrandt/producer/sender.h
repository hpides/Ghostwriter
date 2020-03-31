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

class Sender {
 public:
  Sender(ConnectionManager &connection_manager,
         MessageGenerator &message_generator,
         RequestProcessor &request_processor,
         UCP::Worker &worker,
         ProducerConfig &config);
  void Send(Batch *batch);
 private:
  ProducerConfig &config_;
  ConnectionManager &connection_manager_;
  MessageGenerator &message_generator_;
  RequestProcessor &request_processor_;
  UCP::Worker &worker_;
  uint64_t Stage(Batch *batch);
  void Store(Batch *batch, uint64_t offset);
  bool Commit(Batch *batch, uint64_t offset);
  UCP::Endpoint &GetEndpointWithRKey() const;
  void SendMessage(Message &message, UCP::Endpoint &endpoint);
  uint64_t ReceiveStagedOffset(UCP::Endpoint &endpoint);
  bool ReceiveCommitResponse(UCP::Endpoint &endpoint);
  void WaitUntilReadyToReceive(UCP::Endpoint &endpoint);
  uint64_t message_counter_ = 0;
};

#endif //REMBRANDT_SRC_PRODUCER_SENDER_H_
