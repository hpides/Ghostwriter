#include "../../include/rembrandt/producer/message_accumulator.h"

#include <stdexcept>
#include <boost/functional/hash.hpp>

MessageAccumulator::MessageAccumulator(size_t memory_size, size_t batch_size)
    : batch_size_(batch_size) {
  std::vector<char> v(memory_size);
  buffer_pool_.add_block(&v.front(), v.size(), batch_size);
}

void MessageAccumulator::Append(TopicPartition topic_partition,
                                char *data,
                                size_t size) {
  BatchMap::iterator it = batches_.find(topic_partition);
  std::shared_ptr<BatchDeque> batch_deque;
  Batch *batch;
  if (it == batches_.end()) {
    batch_deque = std::make_shared<std::deque<Batch *>>();
    batch = CreateBatch(topic_partition);
    // TODO: Handle data too large for batch
    batch->append(data, size);
    batch_deque->push_back(batch);
  } else if (it->second->empty() | !it->second->back()->isOpen()) {
    batch = CreateBatch(topic_partition);
    batch->append(data, size);
  } else if (!it->second->back()->hasSpace(size)) {
    it->second->back()->Close();
    batch = CreateBatch(topic_partition);
  } else {
    batch = it->second->back();
  }
  batch->append(data, size);
}

BatchDeque MessageAccumulator::Drain() {
  throw std::logic_error("Not implemented");
}

Batch *MessageAccumulator::CreateBatch(TopicPartition topic_partition) {
  char *buffer = (char *) buffer_pool_.malloc();
  return new Batch(topic_partition, buffer, batch_size_);
}

void MessageAccumulator::FreeBatch(Batch *batch) {
  buffer_pool_.free(batch->getBuffer());
}

