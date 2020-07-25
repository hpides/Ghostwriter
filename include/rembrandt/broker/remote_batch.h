#ifndef REMBRANDT_SRC_BROKER_REMOTE_BATCH_H_
#define REMBRANDT_SRC_BROKER_REMOTE_BATCH_H_

struct RemoteBatch {
  uint64_t logical_offset_;
  uint64_t remote_location_;
  uint64_t effective_message_size_;
  uint64_t batch_size_;
  RemoteBatch() = default;
  RemoteBatch(uint64_t logical_offset,
              uint64_t remote_location,
              uint64_t effective_message_size,
              uint64_t batch_size) : logical_offset_(logical_offset),
                                     remote_location_(remote_location),
                                     effective_message_size_(effective_message_size),
                                     batch_size_(batch_size) {};
};

#endif //REMBRANDT_SRC_BROKER_REMOTE_BATCH_H_
