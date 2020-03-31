#include <iostream>
#include <cstring>
#include "../../include/rembrandt/producer/batch.h"

Batch::Batch(TopicPartition topic_partition,
             char *buffer,
             size_t buffer_length)
    : topic_partition_(topic_partition),
      buffer_(buffer),
      buffer_length_(buffer_length) {}

Batch::Batch(TopicPartition topic_partition,
             char *buffer,
             size_t buffer_length,
             size_t size) : topic_partition_(topic_partition),
                            buffer_(buffer),
                            buffer_length_(buffer_length),
                            size_(size),
                            num_messages_(1) {}

bool Batch::append(char *data_, size_t size) {
  if (hasSpace(size)) {
    memcpy(buffer_ + size_, data_, size);
    size_ += size;
    num_messages_++;
    return true;
  } else {
    return false;
  }
}

bool Batch::hasSpace(size_t size) {
  return (size_ + size <= buffer_length_);
}