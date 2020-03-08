#ifndef REMBRANDT_SRC_PROTOCOL_MESSAGE_GENERATOR_H_
#define REMBRANDT_SRC_PROTOCOL_MESSAGE_GENERATOR_H_

#include "../producer/batch.h"
#include "../network/message.h"
#include "flatbuffers/rembrandt_protocol_generated.h"

class MessageGenerator {
 public:
  MessageGenerator() : builder_(128) {};
  Message Stage(Batch *batch);
  Message StageFailed(const Rembrandt::Protocol::BaseMessage *stage_request);
  Message Staged(const Rembrandt::Protocol::BaseMessage *stage_request, uint64_t offset);
 private:
  flatbuffers::FlatBufferBuilder builder_;
  uint64_t message_counter_ = 0;
  Message CreateMessage(flatbuffers::Offset<Rembrandt::Protocol::BaseMessage> &message);
};

#endif //REMBRANDT_SRC_PROTOCOL_MESSAGE_GENERATOR_H_
