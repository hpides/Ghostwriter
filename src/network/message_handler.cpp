#include <memory>
#include "rembrandt/network/message_handler.h"

MessageHandler::MessageHandler(std::unique_ptr<MessageGenerator> message_generator) : message_generator_p_(std::move(message_generator)) {}

std::unique_ptr<Message> MessageHandler::HandleInitializeRequest(const Rembrandt::Protocol::BaseMessage &initialize_request) {
  return message_generator_p_->InitializeResponse(initialize_request);
}
