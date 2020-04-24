#ifndef REMBRANDT_SRC_BENCHMARK_PARALLEL_DATA_GENERATOR_H_
#define REMBRANDT_SRC_BENCHMARK_PARALLEL_DATA_GENERATOR_H_

#include <atomic>
#include <thread>
#include <tbb/concurrent_queue.h>
#include <vector>
#include <condition_variable>
#include "rate_limiter.h"
#include "data_generator.h"
class ParallelDataGenerator {
 public:
  ParallelDataGenerator(size_t batch_size,
                        tbb::concurrent_bounded_queue<char *> &free,
                        tbb::concurrent_bounded_queue<char *> &generated,
                        RateLimiter &rate_limiter,
                        uint64_t min_key,
                        uint64_t max_key,
                        size_t num_threads,
                        MODE mode);

  void Start(size_t batch_count);
  void StartDataGenerator(DataGenerator &data_generator, size_t batch_count);
  void Stop();
 private:
  std::vector<std::unique_ptr<DataGenerator>> data_generators_;
  size_t num_threads_;
  std::vector<std::thread> threads_;
  std::mutex mutex_;
  std::condition_variable condition_variable_;
  std::atomic<size_t> counter_;
  std::atomic<size_t> waiting_;
};

#endif //REMBRANDT_SRC_BENCHMARK_PARALLEL_DATA_GENERATOR_H_
