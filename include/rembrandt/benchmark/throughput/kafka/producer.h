#ifndef REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_THROUGHPUT_KAFKA_PRODUCER_H_
#define REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_THROUGHPUT_KAFKA_PRODUCER_H_

#include <memory>
#include <unordered_set>
#include <rdkafkacpp.h>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>
#include <rembrandt/benchmark/common/parallel_data_generator.h>
#include <rembrandt/benchmark/kafka/config.h>


class ThroughputLoggingDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  explicit ThroughputLoggingDeliveryReportCb(std::atomic<size_t> &counter,
                                          tbb::concurrent_bounded_queue<char *> &free_buffers) :
      counter_(counter),
      free_buffers_(free_buffers) {}
  void dr_cb(RdKafka::Message &message) override {
    if (message.err()) {
      exit(1);
    } else {
      ++counter_;
      free_buffers_.push((char *) message.msg_opaque());
    }
  }
 private:
  std::atomic<size_t> &counter_;
  tbb::concurrent_bounded_queue<char *> &free_buffers_;
};

class ThroughputKafkaProducer{
 public:
  ThroughputKafkaProducer(int argc, char *const *argv);
  void Run();

 private:
  void ParseOptions(int argc, char *const *argv);
  void ConfigureKafka();
  size_t GetBatchCount();
  size_t GetRunBatchCount();
  size_t GetWarmupBatchCount();
  ProducerConfig config_;
  std::unique_ptr<RdKafka::Conf> kconfig_p_;
  std::unique_ptr<RdKafka::Producer> producer_p_;
  std::unique_ptr<std::unordered_set<std::unique_ptr<char>>> buffers_p_;
  std::unique_ptr<tbb::concurrent_bounded_queue<char *>> free_buffers_p_;
  std::unique_ptr<tbb::concurrent_bounded_queue<char *>> generated_buffers_p_;
  std::unique_ptr<ParallelDataGenerator> warmup_generator_p_;
  std::unique_ptr<ParallelDataGenerator> generator_p_;
  std::atomic<size_t> counter_;
  ThroughputLoggingDeliveryReportCb dr_cb_;
};

#endif //REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_THROUGHPUT_KAFKA_PRODUCER_H_
