#include <rembrandt/producer/batch.h>
#include <rembrandt/network/message.h>
#include <flatbuffers/flatbuffers.h>
#include "rembrandt/protocol/message_generator.h"

Message MessageGenerator::Stage(Batch *batch) {
  auto stage = Rembrandt::Protocol::CreateStage(
      builder_,
      batch->getTopic(),
      batch->getPartition(),
      batch->getNumMessages(),
      batch->getSize());
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      message_counter_,
      Rembrandt::Protocol::Message_Stage,
      stage.Union());
  message_counter_++;
  return CreateMessage(message);
}

Message MessageGenerator::CreateMessage(flatbuffers::Offset<Rembrandt::Protocol::BaseMessage> &message) {
  builder_.FinishSizePrefixed(message);
  const flatbuffers::DetachedBuffer detached_buffer = builder_.Release();
  return Message(std::unique_ptr<char>((char *) detached_buffer.data()), detached_buffer.size());
}

Message MessageGenerator::StageFailed(const Rembrandt::Protocol::BaseMessage *stage_request) {
  auto stage_failed_response =
      Rembrandt::Protocol::CreateStageFailed(builder_, 1, builder_.CreateString("Segment is full!\n"));
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      stage_request->message_id(),
      Rembrandt::Protocol::Message_StageFailed,
      stage_failed_response.Union());
  return CreateMessage(message);
}

Message MessageGenerator::Staged(const Rembrandt::Protocol::BaseMessage *stage_request, uint64_t offset) {
  auto staged_response = Rembrandt::Protocol::CreateStaged(
      builder_,
      offset);
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      stage_request->message_id(),
      Rembrandt::Protocol::Message_Staged,
      staged_response.Union());
  return CreateMessage(message);
}
