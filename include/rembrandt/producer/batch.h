#ifndef REMBRANDT_SRC_PRODUCER_BATCH_H_
#define REMBRANDT_SRC_PRODUCER_BATCH_H_

#include <rembrandt/network/message.h>
#include <memory>
#include "../utils.h"

class Batch {
 public:
  Batch(TopicPartition topic_partition, std::unique_ptr<Message> message);
  Batch(TopicPartition topic_partition, std::unique_ptr<Message> message, size_t size);
  char *getBuffer() { return buffer_->GetBuffer(); };
  uint32_t getTopic() { return topic_partition_.first; };
  uint32_t getPartition() { return topic_partition_.second; };
  TopicPartition getTopicPartition() { return topic_partition_; };
  bool isOpen() { return open_; };
  void Close() { open_ = false; };
  bool hasSpace(size_t size);
  bool append(std::unique_ptr<Message> message);
  uint32_t getNumMessages() { return num_messages_; };
  size_t getSize() { return size_; };
 private:
  TopicPartition topic_partition_;
  std::unique_ptr<Message> buffer_;
  size_t size_ = 0;
  bool open_ = true;
  uint32_t num_messages_ = 0;
};

#endif //REMBRANDT_SRC_PRODUCER_BATCH_H_
