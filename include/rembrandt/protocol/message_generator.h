#ifndef REMBRANDT_SRC_PROTOCOL_MESSAGE_GENERATOR_H_
#define REMBRANDT_SRC_PROTOCOL_MESSAGE_GENERATOR_H_

#include <rembrandt/storage/segment.h>
#include "../producer/batch.h"
#include "../network/message.h"
#include "flatbuffers/rembrandt_protocol_generated.h"

class MessageGenerator {
 public:
  MessageGenerator() : builder_(128) { message_counter_ = 0; };
  std::unique_ptr<Message> AllocateRequest(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id);
  std::unique_ptr<Message> AllocateResponse(Segment &segment,
                                            uint64_t offset,
                                            const Rembrandt::Protocol::BaseMessage &allocate_request);
  std::unique_ptr<Message> AllocateException(const Rembrandt::Protocol::BaseMessage &allocate_request);
  std::unique_ptr<Message> CommitRequest(uint32_t topic_id, uint32_t partition_id, uint64_t offset);
  std::unique_ptr<Message> CommitResponse(uint64_t offset, const Rembrandt::Protocol::BaseMessage &commit_request);
  std::unique_ptr<Message> CommitException(const Rembrandt::Protocol::BaseMessage &commit_request);
  std::unique_ptr<Message> FetchRequest(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id);
  std::unique_ptr<Message> FetchResponse(uint64_t start_offset,
                                         uint64_t commit_offset,
                                         bool is_committable,
                                         const Rembrandt::Protocol::BaseMessage &fetch_request);
  std::unique_ptr<Message> FetchException(const Rembrandt::Protocol::BaseMessage &fetch_request);
  std::unique_ptr<Message> InitializeRequest();
  std::unique_ptr<Message> InitializeResponse(const Rembrandt::Protocol::BaseMessage &initialize_request);
  std::unique_ptr<Message> ReadSegmentRequest(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id, bool next);
  std::unique_ptr<Message> ReadSegmentResponse(uint32_t topic_id,
                                               uint32_t partition_id,
                                               uint32_t segment_id,
                                               uint64_t start_offset,
                                               uint64_t commit_offset,
                                               bool is_committable,
                                               const Rembrandt::Protocol::BaseMessage &read_segment_request);
  std::unique_ptr<Message> ReadSegmentException(const Rembrandt::Protocol::BaseMessage &read_segment_request);
  std::unique_ptr<Message> RMemInfoRequest();
  std::unique_ptr<Message> RMemInfoResponse(uint64_t remote_addr,
                                            const std::string &rkey,
                                            const Rembrandt::Protocol::BaseMessage &rmem_info_request);
  std::unique_ptr<Message> StageMessageRequest(Batch *batch);
  std::unique_ptr<Message> StageMessageResponse(uint64_t offset,
                                                const Rembrandt::Protocol::BaseMessage &stage_message_request);
  std::unique_ptr<Message> StageMessageException(const Rembrandt::Protocol::BaseMessage &stage_message_request);
  std::unique_ptr<Message> StageOffsetRequest(uint32_t topic_id,
                                              uint32_t partition_id,
                                              uint32_t segment_id,
                                              uint64_t offset);
  std::unique_ptr<Message> StageOffsetResponse(const Rembrandt::Protocol::BaseMessage &stage_offset_request);
  std::unique_ptr<Message> StageOffsetException(const Rembrandt::Protocol::BaseMessage &stage_offset_request);
  std::unique_ptr<Message> WriteSegmentRequest(uint32_t topic_id,
                                               uint32_t partition_id,
                                               uint32_t segment_id,
                                               bool next);
  std::unique_ptr<Message> WriteSegmentResponse(uint32_t topic_id,
                                                uint32_t partition_id,
                                                uint32_t segment_id,
                                                uint64_t start_offset,
                                                uint64_t write_offset,
                                                uint64_t size,
                                                bool is_writeable,
                                                const Rembrandt::Protocol::BaseMessage &write_segment_request);
  std::unique_ptr<Message> WriteSegmentException(const Rembrandt::Protocol::BaseMessage &write_segment_request);
 private:
  flatbuffers::FlatBufferBuilder builder_;
  uint64_t message_counter_;
  template<typename T>
  std::unique_ptr<Message> CreateRequest(T protocol_message, Rembrandt::Protocol::Message message_type);
  template<typename T>
  std::unique_ptr<Message> CreateResponse(T protocol_message,
                                          Rembrandt::Protocol::Message message_type,
                                          const Rembrandt::Protocol::BaseMessage &request);
  std::unique_ptr<Message> CreateMessage(flatbuffers::Offset<Rembrandt::Protocol::BaseMessage> &message);
};

#endif //REMBRANDT_SRC_PROTOCOL_MESSAGE_GENERATOR_H_
