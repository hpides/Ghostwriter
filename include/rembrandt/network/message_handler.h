#ifndef REMBRANDT_SRC_NETWORK_MESSAGE_HANDLER_H_
#define REMBRANDT_SRC_NETWORK_MESSAGE_HANDLER_H_

#include "message.h"

class MessageHandler {
 public:
  virtual Message HandleMessage(Message &message) = 0;
};

#endif //REMBRANDT_SRC_NETWORK_MESSAGE_HANDLER_H_
