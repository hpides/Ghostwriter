#ifndef REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_YSB_LOCAL_BENCHMARK_H_
#define REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_YSB_LOCAL_BENCHMARK_H_

#include <memory>
#include <unordered_set>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>
#include <rembrandt/consumer/direct_consumer.h>
#include <rembrandt/network/ucx/context.h>
#include "../../../../../src/benchmark/ysb/YahooBenchmark/GhostwriterYSB.cpp"

class YSBGhostwriterConsumer {
 public:
  YSBGhostwriterConsumer(int argc, char *const *argv);
  void Run();

 private:
  void ParseOptions(int argc, char *const *argv);
//  void Warmup();
  ConsumerConfig config_;
  std::unique_ptr<UCP::Context> context_p_;
  std::unique_ptr<Consumer> consumer_p_;
  std::unique_ptr<tbb::concurrent_bounded_queue<char *>> free_buffers_p_;
  std::unique_ptr<tbb::concurrent_bounded_queue<char *>> received_buffers_p_;
  std::unique_ptr<GhostwriterYSB> ysb_p_;
};

#endif //REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_YSB_LOCAL_BENCHMARK_H_

