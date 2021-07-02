#ifndef REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_PRODUCER_H_
#define REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_PRODUCER_H_

#include <memory>
#include <unordered_set>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>
#include <rembrandt/producer/direct_producer.h>
#include <rembrandt/network/ucx/context.h>
#include <rembrandt/benchmark/common/parallel_data_generator.h>

class BenchmarkProducer{
 public:
  BenchmarkProducer(int argc, char *const *argv);
  void Run();

 private:
  void ParseOptions(int argc, char *const *argv);
  void Warmup();
  size_t GetBatchCount();
  size_t GetRunBatchCount();
  size_t GetWarmupBatchCount();
  size_t GetEffectiveBatchSize();
  ProducerConfig config_;
  std::unique_ptr<UCP::Context> context_p_;
  std::unique_ptr<Producer> producer_p_;
  std::unique_ptr<std::unordered_set<std::unique_ptr<char>>> buffers_p_;
  std::unique_ptr<tbb::concurrent_bounded_queue<char *>> free_buffers_p_;
  std::unique_ptr<tbb::concurrent_bounded_queue<char *>> generated_buffers_p_;
  std::unique_ptr<ParallelDataGenerator> warmup_generator_p_;
  std::unique_ptr<ParallelDataGenerator> generator_p_;
};

#endif //REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_PRODUCER_H_
