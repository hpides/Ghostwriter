#include "../include/rembrandt/producer/message_accumulator.h"
#include "gtest/gtest.h"

TEST(MessageAccumulator, Appending) {
  MessageAccumulator message_accumulator = MessageAccumulator(10, 5);
  char *data = (char *) "foo";
  size_t size = strlen(data);
  message_accumulator.Append(TopicPartition(1, 2), data, size);
}

TEST(MessageAccumulator, Draining) {
  MessageAccumulator message_accumulator = MessageAccumulator(10, 5);
  message_accumulator.Drain();
}