#include "../../include/rembrandt/producer/sender.h"

Sender::Sender(MessageAccumulator &message_accumulator) :
    message_accumulator_(message_accumulator) {}

void Sender::Run() {
  while (true) {
    // TODO
  }
}

