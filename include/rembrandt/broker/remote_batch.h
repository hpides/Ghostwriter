#ifndef REMBRANDT_SRC_BROKER_REMOTE_BATCH_H_
#define REMBRANDT_SRC_BROKER_REMOTE_BATCH_H_

struct RemoteBatch {
  uint64_t logical_offset_;
  uint64_t remote_location_;
  uint64_t batch_;
  RemoteBatch(uint64_t logical_offset, uint64_t remote_location, uint64_t batch) : logical_offset_(logical_offset),
                                                                                   remote_location_(remote_location),
                                                                                   batch_(batch) {};
};

#endif //REMBRANDT_SRC_BROKER_REMOTE_BATCH_H_
