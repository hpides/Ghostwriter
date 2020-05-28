#ifndef REMBRANDT_SRC_NETWORK_MESSAGE_HANDLER_H_
#define REMBRANDT_SRC_NETWORK_MESSAGE_HANDLER_H_

#include <rembrandt/protocol/message_generator.h>
#include "message.h"

class MessageHandler {
 public:
  explicit MessageHandler(std::unique_ptr<MessageGenerator> message_generator);
  virtual std::unique_ptr<Message> HandleMessage(const Message &message) = 0;
 protected:
  virtual std::unique_ptr<Message> HandleInitializeRequest(const Rembrandt::Protocol::BaseMessage &initialize_request);
  std::unique_ptr<MessageGenerator> message_generator_;
};

#endif //REMBRANDT_SRC_NETWORK_MESSAGE_HANDLER_H_
