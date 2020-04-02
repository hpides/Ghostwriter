#include "../../include/rembrandt/producer/message_accumulator.h"
#include <stdexcept>
#include <boost/functional/hash.hpp>
#include <rembrandt/network/attached_message.h>

MessageAccumulator::MessageAccumulator(size_t memory_size, size_t batch_size)
    : batch_size_(batch_size) {
  char *buffer = new char[memory_size];
  buffer_pool_.add_block(buffer, memory_size, batch_size);
}

Batch *MessageAccumulator::GetFullBatch() {
  std::unique_lock lck{full_batches_mutex};
  full_batches_cond_.wait(lck, [&] { return !full_batches_.empty(); });
  Batch *batch = full_batches_.front();
  full_batches_.pop_front();
  lck.unlock();
  return batch;
}

void MessageAccumulator::AddFullBatch(Batch *batch) {
  std::scoped_lock lck{full_batches_mutex};
  full_batches_.push_back(batch);
  full_batches_cond_.notify_one();
}

void MessageAccumulator::Append(TopicPartition topic_partition,
                                std::unique_ptr<Message> message) {
  // TODO: Wait if full
  BatchMap::iterator it = batches_.find(topic_partition);
  Batch *batch;
  if (it == batches_.end()) {
    batch = CreateBatch(topic_partition);
    // TODO: Handle data too large for batch
    batches_[topic_partition] = batch;
  } else {
    batch = it->second;
    if (!batch->isOpen()) {
      AddFullBatch(batch);
      batch = CreateBatch(topic_partition);
      batches_[topic_partition];
    } else if (!batch->hasSpace(message->GetSize())) {
      batch->Close();
      AddFullBatch(batch);
      batch = CreateBatch(topic_partition);
      batches_[topic_partition] = batch;
    }
  }
  batch->append(std::move(message));
}

Batch *MessageAccumulator::CreateBatch(TopicPartition topic_partition) {
  std::unique_lock lck{buffer_mutex_};
  buffer_cond_.wait(lck, [&] { return !buffer_pool_.empty(); });
  char *buffer = (char *) buffer_pool_.malloc();
  lck.unlock();
  return new Batch(topic_partition, std::make_unique<AttachedMessage>(buffer, batch_size_));
}

void MessageAccumulator::Free(Batch *batch) {
  std::scoped_lock lck{buffer_mutex_};
  buffer_pool_.free(batch->getBuffer());
  buffer_cond_.notify_one();
  free(batch);
}

