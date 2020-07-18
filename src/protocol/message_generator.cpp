#include <rembrandt/producer/batch.h>
#include <rembrandt/network/message.h>
#include <flatbuffers/flatbuffers.h>
#include <rembrandt/network/flat_buffers_message.h>
#include "rembrandt/protocol/message_generator.h"

std::unique_ptr<Message> MessageGenerator::AllocateRequest(uint32_t topic_id,
                                                           uint32_t partition_id,
                                                           uint32_t segment_id) {
  auto allocate_request = Rembrandt::Protocol::CreateAllocateRequest(
      builder_,
      topic_id,
      partition_id,
      segment_id);
  return CreateRequest(allocate_request, Rembrandt::Protocol::Message_AllocateRequest);
}

std::unique_ptr<Message> MessageGenerator::AllocateResponse(Segment &segment,
                                                            uint64_t offset,
                                                            const Rembrandt::Protocol::BaseMessage &allocate_request) {
  auto allocate_response = Rembrandt::Protocol::CreateAllocateResponse(
      builder_,
      segment.GetSize(),
      // TODO: Adjust for multiple segments
      offset);
  return CreateResponse(allocate_response, Rembrandt::Protocol::Message_AllocateResponse, allocate_request);
}

std::unique_ptr<Message> MessageGenerator::AllocateException(const Rembrandt::Protocol::BaseMessage &allocate_request) {
  auto allocate_exception = Rembrandt::Protocol::CreateAllocateException(
      builder_, 1, builder_.CreateString("Something went wrong!\n"));
  return CreateResponse(allocate_exception, Rembrandt::Protocol::Message_AllocateException, allocate_request);
}

std::unique_ptr<Message> MessageGenerator::StageRequest(uint32_t topic_id,
                                                        uint32_t partition_id,
                                                        uint64_t message_size) {
  auto stage_request = Rembrandt::Protocol::CreateStageRequest(
      builder_,
      topic_id,
      partition_id,
      message_size);
  return CreateRequest(stage_request, Rembrandt::Protocol::Message_StageRequest);
}

std::unique_ptr<Message> MessageGenerator::CommitRequest(uint32_t topic_id,
                                                         uint32_t partition_id,
                                                         uint64_t offset,
                                                         uint64_t message_size) {
  auto commit = Rembrandt::Protocol::CreateCommitRequest(
      builder_,
      topic_id,
      partition_id,
      offset,
      message_size);
  return CreateRequest(commit, Rembrandt::Protocol::Message_CommitRequest);
}

std::unique_ptr<Message> MessageGenerator::CommitResponse(uint64_t offset,
                                                          const Rembrandt::Protocol::BaseMessage &commit_request) {
  auto commit_response = Rembrandt::Protocol::CreateCommitResponse(builder_, offset);
  return CreateResponse(commit_response, Rembrandt::Protocol::Message_CommitResponse, commit_request);
}

std::unique_ptr<Message> MessageGenerator::CommitException(const Rembrandt::Protocol::BaseMessage &commit_request) {
  auto commit_exception =
      Rembrandt::Protocol::CreateCommitException(builder_, 1, builder_.CreateString("Something went wrong!\n"));
  return CreateResponse(commit_exception, Rembrandt::Protocol::Message_CommitException, commit_request);
}

std::unique_ptr<Message> MessageGenerator::FetchRequest(uint32_t topic_id,
                                                        uint32_t partition_id,
                                                        uint64_t logical_offset) {
  auto fetch_request = Rembrandt::Protocol::CreateFetchRequest(
      builder_,
      topic_id,
      partition_id,
      logical_offset);
  return CreateRequest(fetch_request, Rembrandt::Protocol::Message_FetchRequest);
}

std::unique_ptr<Message> MessageGenerator::FetchResponse(uint64_t remote_location,
                                                         uint64_t commit_offset,
                                                         const Rembrandt::Protocol::BaseMessage &fetch_request) {
  auto fetch_response = Rembrandt::Protocol::CreateFetchResponse(
      builder_,
      remote_location,
      commit_offset);
  return CreateResponse(fetch_response, Rembrandt::Protocol::Message_FetchResponse, fetch_request);
}

std::unique_ptr<Message> MessageGenerator::FetchException(const Rembrandt::Protocol::BaseMessage &fetch_request) {
  auto fetch_exception = Rembrandt::Protocol::CreateFetchException(
      builder_,
      1,
      1);
  return CreateResponse(fetch_exception, Rembrandt::Protocol::Message_FetchException, fetch_request);
}

std::unique_ptr<Message> MessageGenerator::InitializeRequest() {
  auto initialize_request =
      Rembrandt::Protocol::CreateInitializeRequest(builder_);
  return CreateRequest(initialize_request, Rembrandt::Protocol::Message_InitializeRequest);
}

std::unique_ptr<Message> MessageGenerator::InitializeResponse(const Rembrandt::Protocol::BaseMessage &initialize_request) {
  auto initialize_response = Rembrandt::Protocol::CreateInitializeResponse(builder_);
  return CreateResponse(initialize_response, Rembrandt::Protocol::Message_InitializeResponse, initialize_request);
}

std::unique_ptr<Message> MessageGenerator::RMemInfoRequest() {
  auto rmem_info_request = Rembrandt::Protocol::CreateRMemInfoRequest(builder_);
  return CreateRequest(rmem_info_request, Rembrandt::Protocol::Message_RMemInfoRequest);
}

std::unique_ptr<Message> MessageGenerator::RMemInfoResponse(uint64_t remote_addr,
                                                            const std::string &rkey,
                                                            const Rembrandt::Protocol::BaseMessage &rmem_info_request) {
  auto rmem_info_response = Rembrandt::Protocol::CreateRMemInfoResponse(
      builder_,
      remote_addr,
      builder_.CreateString(rkey));
  return CreateResponse(rmem_info_response, Rembrandt::Protocol::Message_RMemInfoResponse, rmem_info_request);
}

std::unique_ptr<Message> MessageGenerator::StageException(const Rembrandt::Protocol::BaseMessage &stage_request) {
  auto stage_message_exception =
      Rembrandt::Protocol::CreateStageException(builder_,
                                                1,
                                                builder_.CreateString("Failed to stage message!\n"));
  return CreateResponse(stage_message_exception,
                        Rembrandt::Protocol::Message_StageException,
                        stage_request);
}

std::unique_ptr<Message> MessageGenerator::StageResponse(uint64_t logical_offset,
                                                         uint64_t remote_location,
                                                         const Rembrandt::Protocol::BaseMessage &stage_request) {
  auto stage_message_response = Rembrandt::Protocol::CreateStageResponse(
      builder_,
      logical_offset,
      remote_location);
  return CreateResponse(stage_message_response,
                        Rembrandt::Protocol::Message_StageResponse,
                        stage_request);
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
                                                          const Rembrandt::Protocol::BaseMessage &request) {
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder_,
      request.message_id(),
      message_type,
      protocol_message.Union());
  return CreateMessage(message);
}

std::unique_ptr<Message> MessageGenerator::CreateMessage(flatbuffers::Offset<Rembrandt::Protocol::BaseMessage> &message) {
  builder_.FinishSizePrefixed(message);
  std::unique_ptr<flatbuffers::DetachedBuffer>
      detached_buffer = std::make_unique<flatbuffers::DetachedBuffer>(builder_.Release());
  return std::make_unique<FlatBuffersMessage>(std::move(detached_buffer));
}
