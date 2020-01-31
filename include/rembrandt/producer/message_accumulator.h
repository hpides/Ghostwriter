#ifndef REMBRANDT_SRC_PRODUCER_MESSAGE_ACCUMULATOR_H_
#define REMBRANDT_SRC_PRODUCER_MESSAGE_ACCUMULATOR_H_

#include <deque>
#include <memory>

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
  BatchDeque Drain();
 private:
  size_t batch_size_;
  boost::simple_segregated_storage<size_t> buffer_pool_;
  BatchMap batches_;
  Batch *CreateBatch(TopicPartition topic_partition);
  void FreeBatch(Batch *batch);
};

#endif //REMBRANDT_SRC_PRODUCER_MESSAGE_ACCUMULATOR_H_
