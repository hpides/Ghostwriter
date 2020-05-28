#ifndef REMBRANDT_SRC_PROTOCOL_MESSAGE_GENERATOR_H_
#define REMBRANDT_SRC_PROTOCOL_MESSAGE_GENERATOR_H_

#include <rembrandt/storage/segment.h>
#include "../producer/batch.h"
#include "../network/message.h"
#include "flatbuffers/rembrandt_protocol_generated.h"

class MessageGenerator {
 public:
  MessageGenerator() : builder_(128) { message_counter_ = 0; };
  std::unique_ptr<Message> Allocate(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id);
  std::unique_ptr<Message> Allocated(const Rembrandt::Protocol::BaseMessage *allocate_request,
                                     Segment &segment,
                                     uint64_t offset);
  std::unique_ptr<Message> AllocateFailed(const Rembrandt::Protocol::BaseMessage *allocate_request);
  std::unique_ptr<Message> CommitRequest(Batch *batch, uint64_t offset);
  std::unique_ptr<Message> CommitResponse(const Rembrandt::Protocol::BaseMessage *commit_request, uint64_t offset);
  std::unique_ptr<Message> CommitException(const Rembrandt::Protocol::BaseMessage *commit_request);
  std::unique_ptr<Message> Fetch(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id);
  std::unique_ptr<Message> Fetched(const Rembrandt::Protocol::BaseMessage *fetch_request,
                                   uint64_t start_offset,
                                   uint64_t commit_offset,
                                   bool is_committable);
  std::unique_ptr<Message> FetchFailed(const Rembrandt::Protocol::BaseMessage *fetch_request);
  std::unique_ptr<Message> Initialize();
  std::unique_ptr<Message> Initialized(const Rembrandt::Protocol::BaseMessage *initialize_request);
  std::unique_ptr<Message> Stage(Batch *batch);
  std::unique_ptr<Message> RequestRMemInfo();
  std::unique_ptr<Message> RMemInfo(const Rembrandt::Protocol::BaseMessage *rmem_info_request,
                                    uint64_t remote_addr,
                                    const std::string &rkey);
  std::unique_ptr<Message> StageFailed(const Rembrandt::Protocol::BaseMessage *stage_request);
  std::unique_ptr<Message> Staged(const Rembrandt::Protocol::BaseMessage *stage_request, uint64_t offset);
 private:
  flatbuffers::FlatBufferBuilder builder_;
  uint64_t message_counter_;
  template<typename T>
  std::unique_ptr<Message> CreateRequest(T protocol_message, Rembrandt::Protocol::Message message_type);
  template<typename T>
  std::unique_ptr<Message> CreateResponse(T protocol_message,
                                          Rembrandt::Protocol::Message message_type,
                                          const Rembrandt::Protocol::BaseMessage *request);
  std::unique_ptr<Message> CreateMessage(flatbuffers::Offset<Rembrandt::Protocol::BaseMessage> &message);
};

#endif //REMBRANDT_SRC_PROTOCOL_MESSAGE_GENERATOR_H_
