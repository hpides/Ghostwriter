#ifndef REMBRANDT_SRC_PRODUCER_SENDER_H_
#define REMBRANDT_SRC_PRODUCER_SENDER_H_

#include <thread>
#include <rembrandt/network/ucx/endpoint.h>
#include <rembrandt/network/ucx/client.h>

#include "message_accumulator.h"
#include "producer_config.h"

class Sender {
 public:
  Sender(UCP::Client &client, MessageAccumulator &message_accumulator, ProducerConfig &config);
  void Start(UCP::Endpoint &ep);
  void Run(UCP::Endpoint &ep);
  void Stop();
  void Send(Batch *batch);
 private:
  ProducerConfig &config_;
  bool running = false;
  UCP::Client client_;
  MessageAccumulator &message_accumulator_;
  std::thread thread_;
  uint64_t message_counter_ = 0;
  uint64_t Stage(Batch *batch);
  void Store(Batch *batch, uint64_t offset);
  bool Commit(uint64_t offset);

  ucs_status_t ProcessRequest(void *status_ptr);
};

#endif //REMBRANDT_SRC_PRODUCER_SENDER_H_
