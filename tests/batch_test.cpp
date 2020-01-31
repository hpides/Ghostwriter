#include "../include/rembrandt/producer/batch.h"
#include "gtest/gtest.h"

TEST(Batch, Construction) {
  // SETUP
  size_t buffer_size = 16;
  char *buffer = (char *) malloc(buffer_size);
  TopicPartition topic_partition(1, 2);
  Batch batch = Batch(topic_partition, buffer, buffer_size);

  ASSERT_TRUE(batch.isOpen());
  ASSERT_TRUE(batch.hasSpace(buffer_size));
  ASSERT_FALSE(batch.hasSpace(buffer_size + 1));
  ASSERT_EQ(batch.getTopic(), 1);
  ASSERT_EQ(batch.getPartition(), 2);
  ASSERT_EQ(batch.getTopicPartition(), TopicPartition(1, 2));
  ASSERT_EQ(batch.getNumMessages(), 0);
// TODO: test getSize()
  // APPENDING
  char *data = (char *) "foo";
  size_t data_size = strlen(data);
  batch.append(data, data_size);
  ASSERT_TRUE(batch.hasSpace(13));
  ASSERT_FALSE(batch.hasSpace(14));
  ASSERT_STREQ(buffer, data);
  ASSERT_EQ(batch.getNumMessages(), 1);

  char *data2 = (char *) "bar";
  size_t data_size2 = strlen(data2);
  batch.append(data2, data_size2);
  ASSERT_TRUE(batch.hasSpace(10));
  ASSERT_FALSE(batch.hasSpace(11));
  ASSERT_STREQ(buffer, "foobar");
  ASSERT_EQ(batch.getNumMessages(), 2);

  // CLOSING
  batch.Close();
  ASSERT_FALSE(batch.isOpen());

  // SHUTDOWN
  free(buffer);
}