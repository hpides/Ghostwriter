#ifndef REMBRANDT_SRC_NETWORK_MESSAGE_H_
#define REMBRANDT_SRC_NETWORK_MESSAGE_H_

#include <memory>

class Message {
 public:
  Message(std::unique_ptr<char> buffer, size_t size);
  char *GetBuffer() { return buffer_.get(); };
  size_t GetSize() { return size_; };
  bool IsEmpty() { return size_ == 0; };
 private:
  std::unique_ptr<char> buffer_;
  size_t size_ = 0;
};

#endif //REMBRANDT_SRC_NETWORK_MESSAGE_H_
