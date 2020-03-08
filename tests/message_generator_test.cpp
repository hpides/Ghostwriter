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
  Batch batch = MockBatch(TopicPartition(1, 1), buffer, 128, "Lorem ipsum dolor sit amet.");
  MessageGenerator message_generator;
  Message stage_message = message_generator.Stage(&batch);
  auto base_message = flatbuffers::GetSizePrefixedRoot<Rembrandt::Protocol::BaseMessage>(stage_message.GetBuffer());
  ASSERT_EQ(base_message->content_type(), Rembrandt::Protocol::Message_Stage);
}
