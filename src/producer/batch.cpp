#include <iostream>
#include <cstring>
#include "../../include/rembrandt/producer/batch.h"

Batch::Batch(TopicPartition topic_partition, std::unique_ptr<Message> message) : topic_partition_(topic_partition),
                                                                                 buffer_(std::move(message)) {}

Batch::Batch(TopicPartition topic_partition,
             std::unique_ptr<Message> message,
             size_t size) : topic_partition_(topic_partition),
                            buffer_(std::move(message)),
                            size_(size),
                            num_messages_(1) {}

bool Batch::append(std::unique_ptr<Message> message) {
  if (hasSpace(message->GetSize())) {
    memcpy(buffer_->GetBuffer() + size_, message->GetBuffer(), message->GetSize());
    size_ += message->GetSize();
    num_messages_++;
    return true;
  } else {
    return false;
  }
}

bool Batch::hasSpace(size_t size) {
  return (size_ + size <= buffer_->GetSize());
}