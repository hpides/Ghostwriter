#ifndef REMBRANDT_SRC_PRODUCER_BATCH_H_
#define REMBRANDT_SRC_PRODUCER_BATCH_H_

#include <cstring>
#include <cstdint>

#include "../utils.h"

class Batch {
 public:
  Batch(TopicPartition topic_partition, char *buffer, size_t buffer_length);
  char *getBuffer() { return buffer_; };
  uint32_t getTopic() { return topic_partition_.first; };
  uint32_t getPartition() { return topic_partition_.second; };
  TopicPartition getTopicPartition() { return topic_partition_; };
  bool isOpen() { return open_; };
  void Close() { open_ = false; };
  bool hasSpace(size_t size);
  bool append(char *data_, size_t size);
  uint32_t getNumMessages() { return num_messages_; };
  size_t getSize() { return size_; };
 private:
  TopicPartition topic_partition_;
  char *buffer_;
  size_t buffer_length_;
  size_t size_ = 0;
  bool open_ = true;
  uint32_t num_messages_ = 0;
};

#endif //REMBRANDT_SRC_PRODUCER_BATCH_H_
