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
  std::unique_ptr<Message> message = message_generator_.Allocate(1, 2, 3);
  auto base_message = flatbuffers::GetSizePrefixedRoot<Rembrandt::Protocol::BaseMessage>(message->GetBuffer());
  ASSERT_EQ(Rembrandt::Protocol::Message_Allocate, base_message->content_type());
  auto allocate_data = static_cast<const Rembrandt::Protocol::Allocate *> (base_message->content());
  EXPECT_EQ(1, allocate_data->topic_id());
  EXPECT_EQ(2, allocate_data->partition_id());
  EXPECT_EQ(3, allocate_data->segment_id());
}

TEST_F(MessageGeneratorTest, Allocated) {
  std::unique_ptr<Message> request = message_generator_.Allocate(1, 2, 3);
  auto base_request = flatbuffers::GetSizePrefixedRoot<Rembrandt::Protocol::BaseMessage>(request->GetBuffer());
  void *location = malloc(100);
  Segment segment = Segment(location, 100);

  std::unique_ptr<Message> response = message_generator_.Allocated(base_request, segment, 42);
  auto base_response = flatbuffers::GetSizePrefixedRoot<Rembrandt::Protocol::BaseMessage>(response->GetBuffer());
  ASSERT_EQ(Rembrandt::Protocol::Message_Allocated, base_response->content_type());

  auto allocated_data = static_cast<const Rembrandt::Protocol::Allocated *> (base_response->content());
  EXPECT_EQ(100, allocated_data->size());
  EXPECT_EQ(42, allocated_data->offset());
}

TEST_F(MessageGeneratorTest, Commit) {
  TopicPartition topic_partition(1, 2);
  char *content = (char *) "foo";
  std::unique_ptr<AttachedMessage> batch_message = std::make_unique<AttachedMessage>(content, strlen(content));
  Batch batch = Batch(topic_partition, std::move(batch_message));
  std::unique_ptr<Message> message = message_generator_.CommitRequest(&batch, 42);
  auto base_message = flatbuffers::GetSizePrefixedRoot<Rembrandt::Protocol::BaseMessage>(message->GetBuffer());
  ASSERT_EQ(Rembrandt::Protocol::Message_CommitRequest, base_message->content_type());
  auto commit_data = static_cast<const Rembrandt::Protocol::CommitRequest *> (base_message->content());
  EXPECT_EQ(1, commit_data->topic_id());
  EXPECT_EQ(2, commit_data->partition_id());
  EXPECT_EQ(42, commit_data->offset());
}

TEST_F(MessageGeneratorTest, Committed) {
  TopicPartition topic_partition(1, 2);
  char *content = (char *) "foo";
  std::unique_ptr<AttachedMessage> batch_message = std::make_unique<AttachedMessage>(content, strlen(content));
  Batch batch = Batch(topic_partition, std::move(batch_message));
  std::unique_ptr<Message> request = message_generator_.CommitRequest(&batch, 42);
  auto base_request = flatbuffers::GetSizePrefixedRoot<Rembrandt::Protocol::BaseMessage>(request->GetBuffer());

  std::unique_ptr<Message> response = message_generator_.CommitResponse(base_request, 42);
  auto base_response = flatbuffers::GetSizePrefixedRoot<Rembrandt::Protocol::BaseMessage>(response->GetBuffer());

  ASSERT_EQ(Rembrandt::Protocol::Message_CommitResponse, base_response->content_type());
  auto committed_data = static_cast<const Rembrandt::Protocol::CommitResponse *> (base_response->content());
  EXPECT_EQ(42, committed_data->offset());
}
}