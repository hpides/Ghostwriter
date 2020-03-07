#ifndef REMBRANDT_SRC_PRODUCER_SENDER_H_
#define REMBRANDT_SRC_PRODUCER_SENDER_H_

#include <thread>
#include <rembrandt/network/ucx/endpoint.h>
#include <rembrandt/network/connection_manager.h>
#include <rembrandt/network/request_processor.h>
#include "message_accumulator.h"
#include "producer_config.h"
#include <rembrandt/network/message.h>

class Sender {
 public:
  Sender(ConnectionManager &connection_manager,
         MessageAccumulator &message_accumulator,
         RequestProcessor &request_processor,
         ProducerConfig &config);
  void Start();
  void Run();
  void Stop();
  void Send(Batch *batch);
 private:
  ProducerConfig &config_;
  bool running = false;
  ConnectionManager &connection_manager_;
  MessageAccumulator &message_accumulator_;
  RequestProcessor &request_processor_;
  std::thread thread_;
  uint64_t message_counter_ = 0;
  uint64_t Stage(Batch *batch);
  void Store(Batch *batch, uint64_t offset);
  bool Commit(uint64_t offset);
  Message generateStageMessage(Batch *batch);
  UCP::Endpoint &GetEndpointWithRKey() const;
  void SendStageRequest(Message &stage_message, UCP::Endpoint &endpoint);
  uint64_t ReceiveStagedOffset(UCP::Endpoint &endpoint);
};

#endif //REMBRANDT_SRC_PRODUCER_SENDER_H_
