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
                        std::unique_ptr<RateLimiter> rate_limiter_p,
                        uint64_t min_key,
                        uint64_t max_key,
                        size_t num_threads,
                        MODE mode);

  static std::unique_ptr<ParallelDataGenerator> Create(size_t batch_size,
                                                size_t rate_limit,
                                                uint64_t min_key,
                                                uint64_t max_key,
                                                size_t num_threads,
                                                MODE mode);

  void Start(size_t batch_count,
             tbb::concurrent_bounded_queue<char *> &free,
             tbb::concurrent_bounded_queue<char *> &generated);
  void StartDataGenerator(DataGenerator &data_generator,
                          size_t batch_count,
                          tbb::concurrent_bounded_queue<char *> &free,
                          tbb::concurrent_bounded_queue<char *> &generated);
  void Stop();
 private:
  std::vector<std::unique_ptr<DataGenerator>> data_generators_;
  std::unique_ptr<RateLimiter> rate_limiter_p_;
  size_t num_threads_;
  std::vector<std::thread> threads_;
  std::mutex mutex_;
  std::condition_variable condition_variable_;
  std::atomic<size_t> counter_;
  std::atomic<size_t> waiting_;
};

#endif //REMBRANDT_SRC_BENCHMARK_PARALLEL_DATA_GENERATOR_H_
