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
  std::unique_ptr<Message> Allocated(const Rembrandt::Protocol::BaseMessage *allocate_request, Segment &segment);
  std::unique_ptr<Message> AllocateFailed(const Rembrandt::Protocol::BaseMessage *allocate_request);
  std::unique_ptr<Message> Commit(Batch *batch, uint64_t offset);
  std::unique_ptr<Message> Committed(const Rembrandt::Protocol::BaseMessage *commit_request, uint64_t offset);
  std::unique_ptr<Message> CommitFailed(const Rembrandt::Protocol::BaseMessage *commit_request);
  std::unique_ptr<Message> Fetch(const TopicPartition &topic_partition, uint64_t last_offset, uint32_t max_length);
  std::unique_ptr<Message> Fetched(const Rembrandt::Protocol::BaseMessage *fetch_request,
                                   uint64_t offset,
                                   uint32_t length);
  std::unique_ptr<Message> FetchFailed(const Rembrandt::Protocol::BaseMessage *fetch_request);
  std::unique_ptr<Message> FetchCommittedOffset(const TopicPartition topic_partition);
  std::unique_ptr<Message> FetchedCommittedOffset(const Rembrandt::Protocol::BaseMessage *committed_offset_request,
                                                  uint64_t committed_offset);
  std::unique_ptr<Message> FetchCommittedOffsetFailed(const Rembrandt::Protocol::BaseMessage *committed_offset_request);
  std::unique_ptr<Message> FetchInitial(const TopicPartition topic_partition);
  std::unique_ptr<Message> FetchedInitial(const Rembrandt::Protocol::BaseMessage *initial_offset_request,
                                          uint64_t start_offset,
                                          uint64_t committed_offset);
  std::unique_ptr<Message> FetchInitialFailed(const Rembrandt::Protocol::BaseMessage *initial_offset_request);
  std::unique_ptr<Message> Initialize();
  std::unique_ptr<Message> Initialized();
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
  std::unique_ptr<Message> CreateMessage(flatbuffers::Offset<Rembrandt::Protocol::BaseMessage> &message);
};

#endif //REMBRANDT_SRC_PROTOCOL_MESSAGE_GENERATOR_H_
