#include "../../include/rembrandt/producer/message_accumulator.h"
#include <iostream>
#include <stdexcept>
#include <boost/functional/hash.hpp>

MessageAccumulator::MessageAccumulator(size_t memory_size, size_t batch_size)
    : batch_size_(batch_size) {
  char *buffer = new char[memory_size];
  buffer_pool_.add_block(buffer, memory_size, batch_size);
}

void MessageAccumulator::Append(TopicPartition topic_partition,
                                char *data,
                                size_t size) {
  // TODO: Wait if full
  BatchMap::iterator it = batches_.find(topic_partition);
  std::shared_ptr<BatchDeque> batch_deque;
  Batch *batch;
  if (it == batches_.end()) {
    batch_deque = std::make_shared<std::deque<Batch *>>();
    batch = CreateBatch(topic_partition);
    // TODO: Handle data too large for batch
    batch_deque->push_back(batch);
    batches_[topic_partition] = batch_deque;
  } else {
    batch_deque = it->second;
    if (batch_deque->empty() | !batch_deque->back()->isOpen()) {
      batch = CreateBatch(topic_partition);
      batch_deque->push_back(batch);
    } else if (!batch_deque->back()->hasSpace(size)) {
      batch_deque->back()->Close();
      batch = CreateBatch(topic_partition);
      batch_deque->push_back(batch);
    } else {
      batch = batch_deque->back();
    }
  }
  batch->append(data, size);
}

std::unique_ptr<BatchDeque> MessageAccumulator::Drain() {
  std::unique_ptr<BatchDeque> full_batches = std::make_unique<BatchDeque>();
  for (auto &entry: batches_) {
    BatchDeque *batch_deque = entry.second.get();
    Batch *batch = nullptr;
    while (!batch_deque->empty()) {
      batch = batch_deque->front();
      if (batch->isOpen()) {
        break;
      }
      full_batches->push_back(batch);
      batch_deque->pop_front();
      // TODO
    }
  }
  return full_batches;
}

Batch *MessageAccumulator::CreateBatch(TopicPartition topic_partition) {
  // TODO: Remove busy waiting and make thread-safe
  while (buffer_pool_.empty()) {
  };
  char *buffer = (char *) buffer_pool_.malloc();
  return new Batch(topic_partition, buffer, batch_size_);
}

void MessageAccumulator::Free(Batch *batch) {
  buffer_pool_.free(batch->getBuffer());
  free(batch);
}

