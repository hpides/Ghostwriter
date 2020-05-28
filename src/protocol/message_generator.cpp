#include <rembrandt/producer/batch.h>
#include <rembrandt/network/message.h>
#include <flatbuffers/flatbuffers.h>
#include <rembrandt/network/flat_buffers_message.h>
#include "rembrandt/protocol/message_generator.h"

std::unique_ptr<Message> MessageGenerator::Allocate(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id) {
  auto allocate_request = Rembrandt::Protocol::CreateAllocate(
      builder_,
      topic_id,
      partition_id,
      segment_id);
  return CreateRequest(allocate_request, Rembrandt::Protocol::Message_Allocate);
}

std::unique_ptr<Message> MessageGenerator::Allocated(const Rembrandt::Protocol::BaseMessage *allocate_request,
                                                     Segment &segment,
                                                     uint64_t offset) {
  auto allocate_response = Rembrandt::Protocol::CreateAllocated(
      builder_,
      segment.GetSize(),
      // TODO: Adjust for multiple segments
      offset);
  return CreateResponse(allocate_response, Rembrandt::Protocol::Message_Allocated, allocate_request);
}

std::unique_ptr<Message> MessageGenerator::AllocateFailed(const Rembrandt::Protocol::BaseMessage *allocate_request) {
  auto allocate_exception = Rembrandt::Protocol::CreateAllocateFailed(
      builder_, 1, builder_.CreateString("Something went wrong!\n"));
  return CreateResponse(allocate_exception, Rembrandt::Protocol::Message_AllocateFailed, allocate_request);
}

std::unique_ptr<Message> MessageGenerator::Stage(Batch *batch) {
  auto stage_request = Rembrandt::Protocol::CreateStage(
      builder_,
      batch->getTopic(),
      batch->getPartition(),
      batch->getNumMessages(),
      batch->getSize());
  return CreateRequest(stage_request, Rembrandt::Protocol::Message_Stage);
}

std::unique_ptr<Message> MessageGenerator::CreateMessage(flatbuffers::Offset<Rembrandt::Protocol::BaseMessage> &message) {
  builder_.FinishSizePrefixed(message);
  std::unique_ptr<flatbuffers::DetachedBuffer>
      detached_buffer = std::make_unique<flatbuffers::DetachedBuffer>(builder_.Release());
  return std::make_unique<FlatBuffersMessage>(std::move(detached_buffer));
}

std::unique_ptr<Message> MessageGenerator::CommitRequest(Batch *batch, uint64_t offset) {
  auto commit = Rembrandt::Protocol::CreateCommitRequest(
      builder_,
      batch->getTopic(),
      batch->getPartition(),
      offset + batch->getSize());
  return CreateRequest(commit, Rembrandt::Protocol::Message_CommitRequest);
}

std::unique_ptr<Message> MessageGenerator::CommitResponse(const Rembrandt::Protocol::BaseMessage *commit_request,
                                                          uint64_t offset) {
  auto commit_response = Rembrandt::Protocol::CreateCommitResponse(builder_, offset);
  return CreateResponse(commit_response, Rembrandt::Protocol::Message_CommitResponse, commit_request);
}

std::unique_ptr<Message> MessageGenerator::CommitException(const Rembrandt::Protocol::BaseMessage *commit_request) {
  auto commit_exception =
      Rembrandt::Protocol::CreateCommitException(builder_, 1, builder_.CreateString("Something went wrong!\n"));
  return CreateResponse(commit_exception, Rembrandt::Protocol::Message_CommitException, commit_request);
}

std::unique_ptr<Message> MessageGenerator::Fetch(uint32_t topic_id,
                                                 uint32_t partition_id,
                                                 uint32_t segment_id) {
  auto fetch_request = Rembrandt::Protocol::CreateFetch(
      builder_,
      topic_id,
      partition_id,
      segment_id);
  return CreateRequest(fetch_request, Rembrandt::Protocol::Message_Fetch);
}

std::unique_ptr<Message> MessageGenerator::Fetched(const Rembrandt::Protocol::BaseMessage *fetch_request,
                                                   uint64_t start_offset,
                                                   uint64_t commit_offset,
                                                   bool is_committable) {
  auto fetch_response = Rembrandt::Protocol::CreateFetched(
      builder_,
      start_offset,
      commit_offset,
      is_committable);
  return CreateResponse(fetch_response, Rembrandt::Protocol::Message_Fetched, fetch_request);
}

std::unique_ptr<Message> MessageGenerator::FetchFailed(const Rembrandt::Protocol::BaseMessage *fetch_request) {
  auto fetch_exception = Rembrandt::Protocol::CreateFetchFailed(
      builder_,
      1,
      1);
  return CreateResponse(fetch_exception, Rembrandt::Protocol::Message_FetchFailed, fetch_request);
}

std::unique_ptr<Message> MessageGenerator::Initialize() {
  auto initialize_request =
      Rembrandt::Protocol::CreateInitialize(builder_);
  return CreateRequest(initialize_request, Rembrandt::Protocol::Message_Initialize);
}

std::unique_ptr<Message> MessageGenerator::Initialized(const Rembrandt::Protocol::BaseMessage *initialize_request) {
  auto initialize_response = Rembrandt::Protocol::CreateInitialized(builder_);
  return CreateResponse(initialize_response, Rembrandt::Protocol::Message_Initialized, initialize_request);
}

std::unique_ptr<Message> MessageGenerator::RequestRMemInfo() {
  auto rmem_info_request = Rembrandt::Protocol::CreateRequestRMemInfo(builder_);
  return CreateRequest(rmem_info_request, Rembrandt::Protocol::Message_RequestRMemInfo);
}

std::unique_ptr<Message> MessageGenerator::RMemInfo(const Rembrandt::Protocol::BaseMessage *rmem_info_request,
                                                    uint64_t remote_addr,
                                                    const std::string &rkey) {
  auto rmem_info_response = Rembrandt::Protocol::CreateRMemInfo(
      builder_,
      remote_addr,
      builder_.CreateString(rkey));
  return CreateResponse(rmem_info_response, Rembrandt::Protocol::Message_RMemInfo, rmem_info_request);
}

std::unique_ptr<Message> MessageGenerator::StageFailed(const Rembrandt::Protocol::BaseMessage *stage_request) {
  auto stage_exception =
      Rembrandt::Protocol::CreateStageFailed(builder_, 1, builder_.CreateString("Segment is full!\n"));
  return CreateResponse(stage_exception, Rembrandt::Protocol::Message_StageFailed, stage_request);
}

std::unique_ptr<Message> MessageGenerator::Staged(const Rembrandt::Protocol::BaseMessage *stage_request,
                                                  uint64_t offset) {
  auto stage_response = Rembrandt::Protocol::CreateStaged(
      builder_,
      offset);
  return CreateResponse(stage_response, Rembrandt::Protocol::Message_Staged, stage_request);
}

template<typename T>
std::unique_ptr<Message> MessageGenerator::CreateRequest(T protocol_message,
                                                         Rembrandt::Protocol::Message message_type) {
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      message_counter_,
      message_type,
      protocol_message.Union());
  message_counter_++;
  return CreateMessage(message);
}

template<typename T>
std::unique_ptr<Message> MessageGenerator::CreateResponse(T protocol_message,
                                                          Rembrandt::Protocol::Message message_type,
                                                          const Rembrandt::Protocol::BaseMessage *request) {
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      request->message_id(),
      message_type,
      protocol_message.Union());
  return CreateMessage(message);
}