#ifndef REMBRANDT_SRC_PRODUCER_SENDER_H_
#define REMBRANDT_SRC_PRODUCER_SENDER_H_

#include "message_accumulator.h"

class Sender {
 public:
  Sender(MessageAccumulator &message_accumulator);
  void Run();
 private:
  MessageAccumulator &message_accumulator_;
  void SendOutline(Batch *batch);
};

#endif //REMBRANDT_SRC_PRODUCER_SENDER_H_
