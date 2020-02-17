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
  void Send(Batch *batch, UCP::Endpoint &endpoint);
 private:
  ProducerConfig &config_;
  uint64_t offset_ = 0;
  bool running = false;
  UCP::Client client_;
  MessageAccumulator &message_accumulator_;
  std::thread thread_;
  void SendOutline(Batch *batch);
//  UCP::Client client_;

};

#endif //REMBRANDT_SRC_PRODUCER_SENDER_H_
