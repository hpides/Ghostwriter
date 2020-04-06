#ifndef REMBRANDT_SRC_NETWORK_MESSAGE_H_
#define REMBRANDT_SRC_NETWORK_MESSAGE_H_

#include <cstdlib>

class Message {
 public:
  virtual char *GetBuffer() const = 0;
  virtual size_t GetSize() const = 0;
  virtual bool IsEmpty() const= 0;
 private:
};

#endif //REMBRANDT_SRC_NETWORK_MESSAGE_H_
