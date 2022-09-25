#ifndef REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_CONSUMER_H_
#define REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_CONSUMER_H_

#include <memory>
#include <unordered_set>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>
#include <rembrandt/consumer/direct_consumer.h>
#include <rembrandt/network/ucx/context.h>
#include <rembrandt/benchmark/common/parallel_data_processor.h>

class BenchmarkConsumer {
 public:
  BenchmarkConsumer(int argc, char *const *argv);
  void Run();

 private:
  void ParseOptions(int argc, char *const *argv);
  void Warmup();
  size_t GetBatchCount();
  size_t GetRunBatchCount();
  size_t GetWarmupBatchCount();
  size_t GetEffectiveBatchSize();
  ConsumerConfig config_;
  std::unique_ptr<UCP::Context> context_p_;
  std::unique_ptr<Consumer> consumer_p_;
  std::unique_ptr<tbb::concurrent_hash_map<uint64_t, uint64_t>> counts_p_;
  std::unique_ptr<tbb::concurrent_bounded_queue<char *>> free_buffers_p_;
  std::unique_ptr<tbb::concurrent_bounded_queue<char *>> received_buffers_p_;
  std::unique_ptr<ParallelDataProcessor> warmup_processor_p_;
  std::unique_ptr<ParallelDataProcessor> processor_p_;
};

#endif //REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_CONSUMER_H_
