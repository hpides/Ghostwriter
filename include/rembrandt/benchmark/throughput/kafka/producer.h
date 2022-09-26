#ifndef REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_PRODUCER_H_
#define REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_PRODUCER_H_

#include <memory>
#include <unordered_set>
#include <rdkafkacpp.h>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>
#include <rembrandt/benchmark/common/parallel_data_generator.h>

struct ProducerConfig {
  std::string broker_node_ip = "10.10.0.1";
  uint16_t broker_node_port = 13360;
  size_t send_buffer_size = 16;
  size_t max_batch_size;
  size_t data_size = 1024l * 1024 * 1024 * 80;
  float warmup_fraction = 0.1;
  size_t rate_limit = 1000l * 1000 * 1000 * 10;
  std::string log_directory = "/tmp/log/kafka";
};

class BenchmarkProducer{
 public:
  BenchmarkProducer(int argc, char *const *argv);
  void Run();

 private:
  void ParseOptions(int argc, char *const *argv);
  void ConfigureKafka();
  void Warmup();
  size_t GetBatchCount();
  size_t GetRunBatchCount();
  size_t GetWarmupBatchCount();
  ProducerConfig config_;
  RdKafka::Conf *kconfig_;

  std::atomic<bool> running_;
  std::unique_ptr<RdKafka::Producer> producer_p_;
  std::unique_ptr<std::unordered_set<std::unique_ptr<char>>> buffers_p_;
  std::unique_ptr<tbb::concurrent_bounded_queue<char *>> free_buffers_p_;
  std::unique_ptr<tbb::concurrent_bounded_queue<char *>> generated_buffers_p_;
  std::unique_ptr<ParallelDataGenerator> warmup_generator_p_;
  std::unique_ptr<ParallelDataGenerator> generator_p_;
  std::unique_ptr<std::thread> polling_thread_p_;
};

#endif //REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_PRODUCER_H_
