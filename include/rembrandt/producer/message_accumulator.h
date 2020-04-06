#ifndef REMBRANDT_SRC_PRODUCER_MESSAGE_ACCUMULATOR_H_
#define REMBRANDT_SRC_PRODUCER_MESSAGE_ACCUMULATOR_H_

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>

#include <boost/functional/hash.hpp>
#include <boost/pool/simple_segregated_storage.hpp>
#include <unordered_map>

#include "../utils.h"
#include "batch.h"

typedef std::deque<Batch *> BatchDeque;
typedef std::unordered_map<TopicPartition, Batch *, boost::hash<TopicPartition>> BatchMap;

class MessageAccumulator {
 public:
  MessageAccumulator(size_t memory_size, size_t batch_size);
  void Append(const TopicPartition &topic_partition, std::unique_ptr<Message> message);
  Batch *GetFullBatch();
  void Free(Batch *batch);
 private:
  std::condition_variable buffer_cond_;
  std::mutex buffer_mutex_;
  std::condition_variable full_batches_cond_;
  std::mutex full_batches_mutex;
  size_t batch_size_;
  boost::simple_segregated_storage<size_t> buffer_pool_;
  BatchMap batches_;
  BatchDeque full_batches_;
  Batch *CreateBatch(const TopicPartition topic_partition);
  void AddFullBatch(Batch *batch);
};

#endif //REMBRANDT_SRC_PRODUCER_MESSAGE_ACCUMULATOR_H_
