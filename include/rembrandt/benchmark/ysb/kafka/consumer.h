#ifndef REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_YSB_GHOSTWRITER_CONSUMER_H_
#define REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_YSB_GHOSTWRITER_CONSUMER_H_

#include <memory>
#include <unordered_set>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>
#include <rdkafkacpp.h>
#include "../../../../../src/benchmark/ysb/YahooBenchmark/KafkaYSB.cpp"
#include <rembrandt/benchmark/kafka/config.h>


class YSBKafkaConsumer {
 public:
  YSBKafkaConsumer(int argc, char *const *argv);
  void Run();

 private:
  void ParseOptions(int argc, char *const *argv);
  size_t GetBatchCount();
  size_t GetRunBatchCount();
  size_t GetWarmupBatchCount();
  size_t GetBatchSize();
  void ConfigureKafka();
  ConsumerConfig config_;
  std::unique_ptr<RdKafka::Conf> kconfig_p_;
  std::unique_ptr<RdKafka::Conf> kconfig_topic_p_;
  std::unique_ptr<RdKafka::Consumer> consumer_p_;
  std::unique_ptr<RdKafka::Topic> topic_p_;
  std::unique_ptr<tbb::concurrent_hash_map<uint64_t, uint64_t>> counts_p_;
  std::unique_ptr<tbb::concurrent_bounded_queue<RdKafka::Message *>> received_messages_p_;
  std::unique_ptr<KafkaYSB> ysb_p_;
};

#endif //REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_YSB_GHOSTWRITER_CONSUMER_H_
