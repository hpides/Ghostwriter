#ifndef REMBRANDT_SRC_NETWORK_ATTACHED_MESSAGE_H_
#define REMBRANDT_SRC_NETWORK_ATTACHED_MESSAGE_H_

#include <cstddef>
#include "message.h"

class AttachedMessage : public Message {
 public:
  AttachedMessage(char *buffer, size_t size);
  char *GetBuffer() override { return buffer_; };
  size_t GetSize() override { return size_; };
  bool IsEmpty() override { return GetSize() == 0; };
 private:
  char *buffer_;
  size_t size_ = 0;
};

#endif //REMBRANDT_SRC_NETWORK_ATTACHED_MESSAGE_H_
