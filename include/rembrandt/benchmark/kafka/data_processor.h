#ifndef REMBRANDT_SRC_BENCHMARK_KAFKA_KAFKA_DATA_PROCESSOR_H_
#define REMBRANDT_SRC_BENCHMARK_KAFKA_KAFKA_DATA_PROCESSOR_H_

#include <atomic>
#include <vector>
#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_hash_map.h>
#include <thread>
#include <rdkafkacpp.h>

class KafkaDataProcessor {
 public:
  KafkaDataProcessor(size_t batch_size,
                tbb::concurrent_bounded_queue<RdKafka::Message *> &received,
                tbb::concurrent_hash_map<uint64_t, uint64_t> &counts);
  void ProcessBatch(char *buffer);
  void Run(size_t batch_count);
  void SetRunning() { running_ = true; };
  void Stop();
 private:
  size_t batch_counter_;
  const size_t batch_size_;
  tbb::concurrent_bounded_queue<RdKafka::Message *> &received_;
  std::vector<uint64_t> local_counts_;
  tbb::concurrent_hash_map<uint64_t, uint64_t> &counts_;
  std::atomic<bool> running_;
};

#endif //REMBRANDT_SRC_BENCHMARK_KAFKA_KAFKA_DATA_PROCESSOR_H_
