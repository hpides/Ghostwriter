#include <rembrandt/network/attached_message.h>
#include "rembrandt/protocol/message_generator.h"
#include "gtest/gtest.h"

namespace {
class MessageGeneratorTest : public testing::Test {
 public:
  MessageGeneratorTest() : message_generator_() {};
 protected:
  MessageGenerator message_generator_;
};

TEST_F(MessageGeneratorTest, Allocate) {
  TopicPartition topic_partition(1, 2);

  std::unique_ptr<Message> message = message_generator_.Allocate(topic_partition);
  auto base_message = flatbuffers::GetSizePrefixedRoot<Rembrandt::Protocol::BaseMessage>(message->GetBuffer());
  ASSERT_EQ(Rembrandt::Protocol::Message_Allocate, base_message->content_type());
  auto allocate_data = static_cast<const Rembrandt::Protocol::Allocate *> (base_message->content());
  EXPECT_EQ(1, allocate_data->topic_id());
  EXPECT_EQ(2, allocate_data->partition_id());
}

TEST_F(MessageGeneratorTest, Allocated) {
  TopicPartition topic_partition(1, 2);
  std::unique_ptr<Message> request = message_generator_.Allocate(topic_partition);
  auto base_request = flatbuffers::GetSizePrefixedRoot<Rembrandt::Protocol::BaseMessage>(request->GetBuffer());
  void *location = malloc(100);
  Segment segment = Segment(location, 100);

  std::unique_ptr<Message> response = message_generator_.Allocated(base_request, segment);
  auto base_response = flatbuffers::GetSizePrefixedRoot<Rembrandt::Protocol::BaseMessage>(response->GetBuffer());
  ASSERT_EQ(Rembrandt::Protocol::Message_Allocated, base_response->content_type());

  auto allocated_data = static_cast<const Rembrandt::Protocol::Allocated *> (base_response->content());
  EXPECT_EQ(100, allocated_data->size());
  EXPECT_EQ(segment.GetDataOffset(), allocated_data->data_offset());
}

TEST_F(MessageGeneratorTest, Commit) {
  TopicPartition topic_partition(1, 2);
  char *content = (char *) "foo";
  std::unique_ptr<AttachedMessage> batch_message = std::make_unique<AttachedMessage>(content, strlen(content));
  Batch batch = Batch(topic_partition, std::move(batch_message));
  std::unique_ptr<Message> message = message_generator_.Commit(&batch, 42);
  auto base_message = flatbuffers::GetSizePrefixedRoot<Rembrandt::Protocol::BaseMessage>(message->GetBuffer());
  ASSERT_EQ(Rembrandt::Protocol::Message_Commit, base_message->content_type());
  auto commit_data = static_cast<const Rembrandt::Protocol::Commit *> (base_message->content());
  EXPECT_EQ(1, commit_data->topic_id());
  EXPECT_EQ(2, commit_data->partition_id());
  EXPECT_EQ(42, commit_data->offset());
}

TEST_F(MessageGeneratorTest, Committed) {
  TopicPartition topic_partition(1, 2);
  char *content = (char *) "foo";
  std::unique_ptr<AttachedMessage> batch_message = std::make_unique<AttachedMessage>(content, strlen(content));
  Batch batch = Batch(topic_partition, std::move(batch_message));
  std::unique_ptr<Message> request = message_generator_.Commit(&batch, 42);
  auto base_request = flatbuffers::GetSizePrefixedRoot<Rembrandt::Protocol::BaseMessage>(request->GetBuffer());

  std::unique_ptr<Message> response = message_generator_.Committed(base_request, 42);
  auto base_response = flatbuffers::GetSizePrefixedRoot<Rembrandt::Protocol::BaseMessage>(response->GetBuffer());

  ASSERT_EQ(Rembrandt::Protocol::Message_Committed, base_response->content_type());
  auto committed_data = static_cast<const Rembrandt::Protocol::Committed *> (base_response->content());
  EXPECT_EQ(42, committed_data->offset());
}

TEST_F(MessageGeneratorTest, FetchCommittedOffset) {
  TopicPartition topic_partition(1, 2);
  std::unique_ptr<Message> message = message_generator_.FetchCommittedOffset(topic_partition);
  auto base_message = flatbuffers::GetSizePrefixedRoot<Rembrandt::Protocol::BaseMessage>(message->GetBuffer());

  ASSERT_EQ(Rembrandt::Protocol::Message_FetchCommittedOffset, base_message->content_type());
  auto fetch_committed_data_offset =
      static_cast<const Rembrandt::Protocol::FetchCommittedOffset *> (base_message->content());
  EXPECT_EQ(1, fetch_committed_data_offset->topic_id());
  EXPECT_EQ(2, fetch_committed_data_offset->partition_id());
}

TEST_F(MessageGeneratorTest, FetchedCommittedOffset) {
  TopicPartition topic_partition(1, 2);
  std::unique_ptr<Message> request = message_generator_.FetchCommittedOffset(topic_partition);
  auto base_request = flatbuffers::GetSizePrefixedRoot<Rembrandt::Protocol::BaseMessage>(request->GetBuffer());

  std::unique_ptr<Message> response = message_generator_.FetchedCommittedOffset(base_request, 42);
  auto base_response = flatbuffers::GetSizePrefixedRoot<Rembrandt::Protocol::BaseMessage>(response->GetBuffer());

  ASSERT_EQ(Rembrandt::Protocol::Message_FetchedCommittedOffset, base_response->content_type());
  auto fetched_committed_offset_data = static_cast<const Rembrandt::Protocol::FetchedCommittedOffset *> (base_response->content());
  EXPECT_EQ(42, fetched_committed_offset_data->offset());
}
}