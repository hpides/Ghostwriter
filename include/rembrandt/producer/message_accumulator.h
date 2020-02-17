#ifndef REMBRANDT_SRC_PRODUCER_MESSAGE_ACCUMULATOR_H_
#define REMBRANDT_SRC_PRODUCER_MESSAGE_ACCUMULATOR_H_

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>

#include <boost/functional/hash.hpp>
#include <boost/pool/simple_segregated_storage.hpp>

#include "../utils.h"
#include "batch.h"

typedef std::deque<Batch *> BatchDeque;
typedef std::unordered_map<TopicPartition,
                           std::shared_ptr<BatchDeque>,
                           boost::hash<TopicPartition>> BatchMap;

class MessageAccumulator {
 public:
  MessageAccumulator(size_t memory_size, size_t batch_size);
  void Append(TopicPartition topic_partition, char *data, size_t size);
  std::unique_ptr<BatchDeque> Drain();
  void Free(Batch *batch);
 private:
  std::condition_variable buffer_cond_;
  std::mutex buffer_mutex_;
  size_t batch_size_;
  boost::simple_segregated_storage<size_t> buffer_pool_;
  BatchMap batches_;
  Batch *CreateBatch(TopicPartition topic_partition);
};

#endif //REMBRANDT_SRC_PRODUCER_MESSAGE_ACCUMULATOR_H_
