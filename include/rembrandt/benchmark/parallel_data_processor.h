#ifndef REMBRANDT_SRC_BENCHMARK_PARALLEL_DATA_PROCESSOR_H_
#define REMBRANDT_SRC_BENCHMARK_PARALLEL_DATA_PROCESSOR_H_

#include <atomic>
#include <thread>
#include <tbb/concurrent_queue.h>
#include <vector>
#include <condition_variable>
#include "rate_limiter.h"
#include "data_processor.h"
#include <tbb/concurrent_hash_map.h>

class ParallelDataProcessor {
 public:
  ParallelDataProcessor(size_t batch_size,
                        tbb::concurrent_bounded_queue<char *> &free,
                        tbb::concurrent_bounded_queue<char *> &received,
                        tbb::concurrent_hash_map<uint64_t, uint64_t> &counts,
                        size_t num_threads);

  void Start(size_t batch_count);
  void StartDataProcessor(DataProcessor &data_processor, size_t batch_count);
  void Stop();
 private:
  std::vector<std::unique_ptr<DataProcessor>> data_processors_;
  size_t num_threads_;
  std::vector<std::thread> threads_;
  std::mutex mutex_;
  std::condition_variable condition_variable_;
  std::atomic<size_t> counter_;
  std::atomic<size_t> waiting_;
  tbb::concurrent_hash_map<uint64_t, uint64_t> counts_;
};

#endif //REMBRANDT_SRC_BENCHMARK_PARALLEL_DATA_PROCESSOR_H_
