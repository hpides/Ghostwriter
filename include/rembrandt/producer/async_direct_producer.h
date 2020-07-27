#ifndef REMBRANDT_SRC_PRODUCER_ASYNC_DIRECT_PRODUCER_H_
#define REMBRANDT_SRC_PRODUCER_ASYNC_DIRECT_PRODUCER_H_

#include "rembrandt/producer/producer_config.h"
#include "../utils.h"
#include "producer.h"
#include "sender.h"
#include <atomic>

typedef std::deque<std::unique_ptr<Batch>> BatchPointerDeque;

class AsyncDirectProducer : public Producer {
 public:
  AsyncDirectProducer(Sender &sender, ProducerConfig &config);
  virtual void Send(const TopicPartition &topic_partition, std::unique_ptr<Message>, uint64_t (*&latencies)) override;
  void Start();
  void Stop();
  void Run();
 private:
  ProducerConfig &config_;
  Sender &sender_;
  std::condition_variable full_batches_cond_;
  std::mutex full_batches_mutex;
  BatchPointerDeque full_batches_;
  std::atomic<bool> running_ = false;
  std::thread thread_;
  std::unique_ptr<Batch> GetFullBatch();
};

#endif //REMBRANDT_SRC_PRODUCER_ASYNC_DIRECT_PRODUCER_H_
