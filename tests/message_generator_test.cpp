#include "gtest/gtest.h"
#include "rembrandt/protocol/flatbuffers/rembrandt_protocol_generated.h"
#include "../include/rembrandt/producer/sender.h"

class MockBatch : public Batch {
 public:
  MockBatch(TopicPartition topic_partition, char *buffer, size_t buffer_length, std::string test_data) :
      Batch(topic_partition, buffer, buffer_length) {
    this->append((char *) test_data.c_str(), test_data.size());
  }
};

TEST(MessageGenerator, Stage) {
  char *buffer = (char *) malloc(128);
  char *test_data = (char *) "Lorem ipsum dolor sit amet.";
  Batch batch = MockBatch(TopicPartition(1, 1), buffer, 128, test_data);
  MessageGenerator message_generator;
  std::unique_ptr<Message> stage_message = message_generator.Stage(&batch);
  auto base_message = flatbuffers::GetSizePrefixedRoot<Rembrandt::Protocol::BaseMessage>(stage_message->GetBuffer());
  ASSERT_EQ(base_message->content_type(), Rembrandt::Protocol::Message_Stage);
  auto stage = static_cast<const Rembrandt::Protocol::Stage *> (base_message->content());
  ASSERT_EQ(stage->total_size(), strlen(test_data));
}
