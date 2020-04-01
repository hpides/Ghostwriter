#ifndef REMBRANDT_SRC_NETWORK_DETACHED_MESSAGE_H_
#define REMBRANDT_SRC_NETWORK_DETACHED_MESSAGE_H_

#include <memory>
#include "message.h"
class DetachedMessage : public Message {
 public:
  DetachedMessage(std::unique_ptr<char> buffer, size_t size);
  char *GetBuffer() override { return buffer_.get(); };
  size_t GetSize() override { return size_; };
  bool IsEmpty() override { return GetSize() == 0; };
 private:
  std::unique_ptr<char> buffer_;
  size_t size_ = 0;
};

#endif //REMBRANDT_SRC_NETWORK_DETACHED_MESSAGE_H_
