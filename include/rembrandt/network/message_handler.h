#ifndef REMBRANDT_SRC_NETWORK_MESSAGE_HANDLER_H_
#define REMBRANDT_SRC_NETWORK_MESSAGE_HANDLER_H_

#include <rembrandt/protocol/message_generator.h>
#include "message.h"

class MessageHandler {
 public:
  explicit MessageHandler(MessageGenerator &message_generator);
  virtual std::unique_ptr<Message> HandleMessage(Message &message) = 0;
 protected:
  virtual std::unique_ptr<Message> HandleInitialize(const Rembrandt::Protocol::BaseMessage *initialize_request);
  MessageGenerator &message_generator_;
};

#endif //REMBRANDT_SRC_NETWORK_MESSAGE_HANDLER_H_
