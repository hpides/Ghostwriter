#ifndef REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_CONSUMER_H_
#define REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_CONSUMER_H_

#include <memory>
#include <unordered_set>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>
#include <rdkafkacpp.h>
#include <rembrandt/benchmark/kafka/parallel_data_processor.h>
#include <rembrandt/benchmark/kafka/config.h>

class BenchmarkConsumer {
 public:
  BenchmarkConsumer(int argc, char *const *argv);
  void Run();

 private:
  void ParseOptions(int argc, char *const *argv);
  void ConfigureKafka();
  void Warmup();
  size_t GetBatchCount();
  size_t GetRunBatchCount();
  size_t GetWarmupBatchCount();
  ConsumerConfig config_;
  std::unique_ptr<RdKafka::Conf> kconfig_p_;
  std::unique_ptr<RdKafka::Conf> kconfig_topic_p_;
  std::unique_ptr<RdKafka::Consumer> consumer_p_;
  std::unique_ptr<RdKafka::Topic> topic_p_;
  std::unique_ptr<tbb::concurrent_hash_map<uint64_t, uint64_t>> counts_p_;
  std::unique_ptr<tbb::concurrent_bounded_queue<RdKafka::Message *>> received_messages_p_;
  std::unique_ptr<KafkaParallelDataProcessor> warmup_processor_p_;
  std::unique_ptr<KafkaParallelDataProcessor> processor_p_;
};

#endif //REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_CONSUMER_H_
