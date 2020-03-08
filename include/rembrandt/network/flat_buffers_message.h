#ifndef REMBRANDT_SRC_NETWORK_FLAT_BUFFERS_MESSAGE_H_
#define REMBRANDT_SRC_NETWORK_FLAT_BUFFERS_MESSAGE_H_

#include <flatbuffers/flatbuffers.h>
#include "message.h"
class FlatBuffersMessage : public Message {
 public:
  FlatBuffersMessage(std::unique_ptr<flatbuffers::DetachedBuffer> detached_buffer);
  char *GetBuffer() override { return (char *) detached_buffer_->data(); };
  size_t GetSize() override { return detached_buffer_->size(); };
  bool IsEmpty() override { return GetSize() == 0; };
 private:
  std::unique_ptr<flatbuffers::DetachedBuffer> detached_buffer_;
};

#endif //REMBRANDT_SRC_NETWORK_FLAT_BUFFERS_MESSAGE_H_
