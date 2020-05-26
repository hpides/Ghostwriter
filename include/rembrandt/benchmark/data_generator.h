#ifndef REMBRANDT_SRC_BENCHMARK_DATA_GENERATOR_H_
#define REMBRANDT_SRC_BENCHMARK_DATA_GENERATOR_H_

#include "rate_limiter.h"
#include <atomic>
#include <tbb/concurrent_queue.h>
#include <thread>

enum MODE {
  STRICT,
  RELAXED
};

class DataGenerator {
 public:
  DataGenerator(size_t batch_size,
                tbb::concurrent_bounded_queue<char *> &free,
                tbb::concurrent_bounded_queue<char *> &generated,
                RateLimiter &rate_limiter,
                uint64_t min_key,
                uint64_t max_key,
                MODE mode);
  void GenerateBatch(char *buffer);
  void Run(size_t batch_count);
  void SetRunning() { running_ = true; };
  void Stop();
 private:
  size_t batch_counter_;
  const size_t batch_size_;
  tbb::concurrent_bounded_queue<char *> &free_;
  tbb::concurrent_bounded_queue<char *> &generated_;
  RateLimiter &rate_limiter_;
  const uint64_t min_key_;
  const uint64_t max_key_;
  char *GetFreeBuffer();
  std::atomic<bool> running_;
  const MODE mode_;
};

#endif //REMBRANDT_SRC_BENCHMARK_DATA_GENERATOR_H_
