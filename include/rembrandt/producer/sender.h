#ifndef REMBRANDT_SRC_PRODUCER_SENDER_H_
#define REMBRANDT_SRC_PRODUCER_SENDER_H_

#include <thread>
#include <rembrandt/network/ucx/endpoint.h>
#include <rembrandt/network/ucx/client.h>

#include "message_accumulator.h"

class Sender {
 public:
  Sender(UCP::Client &client, MessageAccumulator &message_accumulator);
  void Start(UCP::Endpoint &ep);
  void Run(UCP::Endpoint &ep);
  void Stop();
 private:
  bool running = false;
  UCP::Client client_;
  MessageAccumulator &message_accumulator_;
  std::thread thread_;
  void Send(Batch *batch, UCP::Endpoint &endpoint);
  void SendOutline(Batch *batch);
//  UCP::Client client_;

};

#endif //REMBRANDT_SRC_PRODUCER_SENDER_H_
