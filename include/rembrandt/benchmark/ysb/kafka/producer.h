#ifndef REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_YSB_KAFKA_PRODUCER_H_
#define REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_YSB_KAFKA_PRODUCER_H_

#include <memory>
#include <unordered_set>
#include <rdkafkacpp.h>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>
#include <rembrandt/benchmark/kafka/config.h>
#include <iostream>

class ThroughputLoggingDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  explicit ThroughputLoggingDeliveryReportCb(std::atomic<size_t> &counter) :
      counter_(counter) {}
  void dr_cb(RdKafka::Message &message) override {
    if (message.err()) {
      exit(1);
    } else {
      ++counter_;
    }
  }
 private:
  std::atomic<size_t> &counter_;
};

class YSBKafkaProducer{
 public:
  YSBKafkaProducer(int argc, char *const *argv);
  void Run();

 private:
  void ParseOptions(int argc, char *const *argv);
  void ConfigureKafka();
  void ReadIntoMemory();
  size_t GetBatchCount();
  size_t GetRunBatchCount();
  size_t GetWarmupBatchCount();
  size_t GetBatchSize();
  ProducerConfig config_;
  std::unique_ptr<RdKafka::Conf> kconfig_p_;
  std::unique_ptr<RdKafka::Producer> producer_p_;
  std::string input_path_;
  char *input_p_;
  long fsize_;
  std::atomic<size_t> counter_;
  ThroughputLoggingDeliveryReportCb dr_cb_;
};



#endif //REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_YSB_KAFKA_PRODUCER_H_
