#include <rembrandt/producer/batch.h>
#include <rembrandt/network/message.h>
#include <flatbuffers/flatbuffers.h>
#include <rembrandt/network/flat_buffers_message.h>
#include "rembrandt/protocol/message_generator.h"

std::unique_ptr<Message> MessageGenerator::Stage(Batch *batch) {
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

std::unique_ptr<Message> MessageGenerator::CreateMessage(flatbuffers::Offset<Rembrandt::Protocol::BaseMessage> &message) {
  builder_.FinishSizePrefixed(message);
  std::unique_ptr<flatbuffers::DetachedBuffer>
      detached_buffer = std::make_unique<flatbuffers::DetachedBuffer>(builder_.Release());
  return std::make_unique<FlatBuffersMessage>(std::move(detached_buffer));
}

std::unique_ptr<Message> MessageGenerator::Commit(Batch *batch, uint64_t offset) {
  auto commit = Rembrandt::Protocol::CreateCommit(
      builder_,
      batch->getTopic(),
      batch->getPartition(),
      offset);
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      message_counter_,
      Rembrandt::Protocol::Message_Commit,
      commit.Union());
  message_counter_++;
  return CreateMessage(message);
}

std::unique_ptr<Message> MessageGenerator::Committed(const Rembrandt::Protocol::BaseMessage *commit_request,
                                                     uint64_t offset) {
  auto committed_response = Rembrandt::Protocol::CreateCommitted(builder_, offset);
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      commit_request->message_id(),
      Rembrandt::Protocol::Message_Committed,
      committed_response.Union());
  return CreateMessage(message);
}

std::unique_ptr<Message> MessageGenerator::CommitFailed(const Rembrandt::Protocol::BaseMessage *commit_request) {
  auto commit_failed_response =
      Rembrandt::Protocol::CreateCommitFailed(builder_, 1, builder_.CreateString("Something went wrong!\n"));
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      commit_request->message_id(),
      Rembrandt::Protocol::Message_CommitFailed,
      commit_failed_response.Union());
  return CreateMessage(message);
}

std::unique_ptr<Message> MessageGenerator::StageFailed(const Rembrandt::Protocol::BaseMessage *stage_request) {
  auto stage_failed_response =
      Rembrandt::Protocol::CreateStageFailed(builder_, 1, builder_.CreateString("Segment is full!\n"));
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      stage_request->message_id(),
      Rembrandt::Protocol::Message_StageFailed,
      stage_failed_response.Union());
  return CreateMessage(message);
}

std::unique_ptr<Message> MessageGenerator::Staged(const Rembrandt::Protocol::BaseMessage *stage_request,
                                                  uint64_t offset) {
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
