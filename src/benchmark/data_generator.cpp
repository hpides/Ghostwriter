#include <chrono>
#include "rembrandt/benchmark/data_generator.h"

DataGenerator::DataGenerator(size_t batch_size,
                             tbb::concurrent_bounded_queue<char *> &free,
                             tbb::concurrent_bounded_queue<char *> &generated,
                             RateLimiter &rate_limiter,
                             uint64_t min_key,
                             uint64_t max_key) :
    batch_counter_(0),
    batch_size_(batch_size),
    free_(free),
    generated_(generated),
    rate_limiter_(rate_limiter),
    // TODO: Check key range
    min_key_(min_key),
    max_key_(max_key) {}

void DataGenerator::Run(size_t batch_count) {
  char *buffer;
  for (int i = 0; i < batch_count; i++) {
    buffer = GetFreeBuffer();
    GenerateBatch(buffer);
    if (!generated_.try_push(buffer)) {
      throw std::runtime_error("Could not post generated buffer, queue is full.");
    }
  }
}

void DataGenerator::GenerateBatch(char *buffer) {
  static constexpr size_t element_size = sizeof(uint64_t);
  size_t element_count = (batch_size_ - sizeof(long)) / element_size;
  auto timestamp_location = (long *) buffer;
  auto data_location = (uint64_t *) (buffer + sizeof(long));
  uint64_t key_range_size = max_key_ - min_key_;
  uint64_t shift_per_batch = element_count % key_range_size;
  uint64_t element_shift = (shift_per_batch * batch_counter_) % key_range_size;
  for (size_t i = 0; i < element_count; i++) {
    *(data_location + i) = min_key_ + (element_shift + i) % key_range_size;
  }
  auto now = std::chrono::steady_clock::now();
  auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
  *timestamp_location = ts.count();
  batch_counter_++;
}

char *DataGenerator::GetFreeBuffer() {
  char *buffer;
  if (!free_.try_pop(buffer)) {
    throw std::runtime_error("Could not receive free buffer, queue is empty.");
  }
  return buffer;
}