#ifndef REMBRANDT_SRC_NETWORK_MESSAGE_H_
#define REMBRANDT_SRC_NETWORK_MESSAGE_H_

#include <cstdlib>

class Message {
 public:
  virtual char *GetBuffer() = 0;
  virtual size_t GetSize() = 0;
  virtual bool IsEmpty() = 0;
 private:
};

#endif //REMBRANDT_SRC_NETWORK_MESSAGE_H_
