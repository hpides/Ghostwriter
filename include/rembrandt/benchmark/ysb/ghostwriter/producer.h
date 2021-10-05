#ifndef REMBRANDT_INCLUDE_REMBRANDT_YSB_GHOSTWRITER_PRODUCER_H_
#define REMBRANDT_INCLUDE_REMBRANDT_YSB_GHOSTWRITER_PRODUCER_H_

#include <memory>
#include <unordered_set>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>
#include <rembrandt/producer/direct_producer.h>
#include <rembrandt/network/ucx/context.h>

class YSBGhostwriterProducer{
 public:
  YSBGhostwriterProducer(int argc, char *const *argv);
  void Run();

 private:
  void ParseOptions(int argc, char *const *argv);
//  void Warmup();
  void ReadIntoMemory();
  size_t GetBatchCount();
  size_t GetRunBatchCount();
  size_t GetWarmupBatchCount();
  size_t GetBatchSize();
  size_t GetEffectiveBatchSize();
  ProducerConfig config_;
  std::unique_ptr<UCP::Context> context_p_;
  std::unique_ptr<Producer> producer_p_;
  std::string input_path_;
  char *input_p_;
  long fsize_;
};

#endif //REMBRANDT_INCLUDE_REMBRANDT_YSB_GHOSTWRITER_PRODUCER_H_
