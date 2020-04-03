#include <rembrandt/producer/batch.h>
#include <rembrandt/network/message.h>
#include <flatbuffers/flatbuffers.h>
#include <rembrandt/network/flat_buffers_message.h>
#include "rembrandt/protocol/message_generator.h"

std::unique_ptr<Message> MessageGenerator::Allocate(const TopicPartition &topic_partition) {
  auto allocate = Rembrandt::Protocol::CreateAllocate(
      builder_,
      topic_partition.first,
      topic_partition.second,
      1);
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      message_counter_,
      Rembrandt::Protocol::Message_Allocate,
      allocate.Union());
  message_counter_++;
  return std::move(CreateMessage(message));
}

std::unique_ptr<Message> MessageGenerator::Allocated(const Rembrandt::Protocol::BaseMessage *allocate_request,
                                                     Segment &segment) {
  auto allocated = Rembrandt::Protocol::CreateAllocated(
      builder_,
      segment.GetDataOffset(),
      segment.GetSize());
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      allocate_request->message_id(),
      Rembrandt::Protocol::Message_Allocated,
      allocated.Union());
  return std::move(CreateMessage(message));
}

std::unique_ptr<Message> MessageGenerator::AllocateFailed(const Rembrandt::Protocol::BaseMessage *allocate_request) {
  auto allocate_failed = Rembrandt::Protocol::CreateAllocateFailed(
      builder_, 1, builder_.CreateString("Something went wrong!\n"));
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      allocate_request->message_id(),
      Rembrandt::Protocol::Message_AllocateFailed,
      allocate_failed.Union());
  return std::move(CreateMessage(message));
}

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
  return std::move(CreateMessage(message));
}

std::unique_ptr<Message> MessageGenerator::CreateMessage(flatbuffers::Offset<Rembrandt::Protocol::BaseMessage> &message) {
  builder_.FinishSizePrefixed(message);
  std::unique_ptr<flatbuffers::DetachedBuffer>
      detached_buffer = std::make_unique<flatbuffers::DetachedBuffer>(builder_.Release());
  return std::move(std::make_unique<FlatBuffersMessage>(std::move(detached_buffer)));
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

std::unique_ptr<Message> MessageGenerator::Fetch(TopicPartition topic_partition,
                                                 uint64_t last_offset,
                                                 uint32_t max_length) {
  auto fetch = Rembrandt::Protocol::CreateFetch(
      builder_,
      topic_partition.first,
      topic_partition.second,
      last_offset,
      max_length);
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      message_counter_,
      Rembrandt::Protocol::Message_Fetch,
      fetch.Union());
  return CreateMessage(message);
}

std::unique_ptr<Message> MessageGenerator::Fetched(const Rembrandt::Protocol::BaseMessage *fetch_request,
                                                   uint64_t offset,
                                                   uint32_t length) {
  auto fetched = Rembrandt::Protocol::CreateFetched(
      builder_,
      offset,
      length);
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      fetch_request->message_id(),
      Rembrandt::Protocol::Message_Fetched,
      fetched.Union());
  return CreateMessage(message);
}

std::unique_ptr<Message> MessageGenerator::FetchFailed(const Rembrandt::Protocol::BaseMessage *fetch_request) {
  auto fetch_failed = Rembrandt::Protocol::CreateFetchFailed(
      builder_,
      1,
      1);
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      fetch_request->message_id(),
      Rembrandt::Protocol::Message_FetchFailed,
      fetch_failed.Union());
  return CreateMessage(message);
}

std::unique_ptr<Message> MessageGenerator::Initialize() {
  auto initialize =
      Rembrandt::Protocol::CreateInitialize(builder_);
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      message_counter_,
      Rembrandt::Protocol::Message_Initialize,
      initialize.Union());
  return CreateMessage(message);
}

std::unique_ptr<Message> MessageGenerator::Initialized() {
  auto initialized =
      Rembrandt::Protocol::CreateInitialized(builder_);
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      message_counter_,
      Rembrandt::Protocol::Message_Initialized,
      initialized.Union());
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
