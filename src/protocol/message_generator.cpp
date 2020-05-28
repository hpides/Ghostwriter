#include <rembrandt/producer/batch.h>
#include <rembrandt/network/message.h>
#include <flatbuffers/flatbuffers.h>
#include <rembrandt/network/flat_buffers_message.h>
#include <rembrandt/storage/storage_manager.h>
#include "rembrandt/protocol/message_generator.h"

std::unique_ptr<Message> MessageGenerator::Allocate(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id) {
  auto allocate = Rembrandt::Protocol::CreateAllocate(
      builder_,
      topic_id,
      partition_id,
      segment_id);
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      message_counter_,
      Rembrandt::Protocol::Message_Allocate,
      allocate.Union());
  message_counter_++;
  return CreateMessage(message);
}

std::unique_ptr<Message> MessageGenerator::Allocated(const Rembrandt::Protocol::BaseMessage *allocate_request,
                                                     Segment &segment,
                                                     uint64_t offset) {
  auto allocated = Rembrandt::Protocol::CreateAllocated(
      builder_,
      segment.GetSize(),
      // TODO: Adjust for multiple segments
      offset);
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      allocate_request->message_id(),
      Rembrandt::Protocol::Message_Allocated,
      allocated.Union());
  return CreateMessage(message);
}

std::unique_ptr<Message> MessageGenerator::AllocateFailed(const Rembrandt::Protocol::BaseMessage *allocate_request) {
  auto allocate_failed = Rembrandt::Protocol::CreateAllocateFailed(
      builder_, 1, builder_.CreateString("Something went wrong!\n"));
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      allocate_request->message_id(),
      Rembrandt::Protocol::Message_AllocateFailed,
      allocate_failed.Union());
  return CreateMessage(message);
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
      offset + batch->getSize());
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

std::unique_ptr<Message> MessageGenerator::Fetch(uint32_t topic_id,
                                                 uint32_t partition_id,
                                                 uint32_t segment_id) {
  auto fetch = Rembrandt::Protocol::CreateFetch(
      builder_,
      topic_id,
      partition_id,
      segment_id);
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      message_counter_,
      Rembrandt::Protocol::Message_Fetch,
      fetch.Union());
  return CreateMessage(message);
}

std::unique_ptr<Message> MessageGenerator::Fetched(const Rembrandt::Protocol::BaseMessage *fetch_request,
                                                   uint64_t start_offset,
                                                   uint64_t commit_offset,
                                                   bool is_committable) {
  auto fetched = Rembrandt::Protocol::CreateFetched(
      builder_,
      start_offset,
      commit_offset,
      is_committable);
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
      message_counter_++,
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

std::unique_ptr<Message> MessageGenerator::RequestRMemInfo() {
  auto request = Rembrandt::Protocol::CreateRequestRMemInfo(builder_);
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      message_counter_++,
      Rembrandt::Protocol::Message_RequestRMemInfo,
      request.Union());
  return CreateMessage(message);
}

std::unique_ptr<Message> MessageGenerator::RMemInfo(const Rembrandt::Protocol::BaseMessage *rmem_info_request,
                                                    uint64_t remote_addr,
                                                    const std::string &rkey) {
  auto rmem_info = Rembrandt::Protocol::CreateRMemInfo(
      builder_,
      remote_addr,
      builder_.CreateString(rkey));
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      rmem_info_request->message_id(),
      Rembrandt::Protocol::Message_RMemInfo,
      rmem_info.Union());
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
