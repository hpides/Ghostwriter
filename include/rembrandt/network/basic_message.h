#ifndef REMBRANDT_SRC_NETWORK_BASIC_MESSAGE_H_
#define REMBRANDT_SRC_NETWORK_BASIC_MESSAGE_H_

#include <memory>
#include "message.h"
class BasicMessage : public Message {
 public:
  BasicMessage(std::unique_ptr<char> buffer, size_t size);
  char *GetBuffer() override { return buffer_.get(); };
  size_t GetSize() override { return size_; };
 private:
  std::unique_ptr<char> buffer_;
  size_t size_ = 0;
};

#endif //REMBRANDT_SRC_NETWORK_BASIC_MESSAGE_H_
