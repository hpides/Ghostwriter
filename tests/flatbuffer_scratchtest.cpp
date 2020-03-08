#include "rembrandt/protocol/flatbuffers/rembrandt_protocol_generated.h"
#import "gtest/gtest.h"

TEST(Bla, Foo) {

  uint8_t *buf;
  int size;
  {
    flatbuffers::FlatBufferBuilder builder(128);
    auto send_outline = Rembrandt::Protocol::CreateSendOutline(
        builder,
        1,
        2,
        3,
        4);
    auto message = Rembrandt::Protocol::CreateBaseMessage(
        builder,
        Rembrandt::Protocol::Message_SendOutline,
        send_outline.Union());
    builder.Finish(message);
    auto foo = builder.Release()
    buf = builder.GetBufferPointer();
    size = builder.GetSize();
    printf("Alignment: %d\n", builder.GetBufferMinAlignment());
    printf("Size: %d\n", builder.GetSize());
  }

  {
    auto base_message =
        flatbuffers::GetRoot<Rembrandt::Protocol::BaseMessage>(buf);
    auto union_type = base_message->content_type();
    if (union_type == Rembrandt::Protocol::Message_SendOutline) {
      auto send_outline =
          static_cast<const Rembrandt::Protocol::SendOutline *> (base_message->content());
      printf("Topic: %d\n", send_outline->topic_id());
      printf("Partition: %d\n", send_outline->partition_id());
      printf("Number of Messages: %d\n", send_outline->num_messages());
      printf("Size: %d\n", send_outline->total_size());
    }
  }
}