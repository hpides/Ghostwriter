#include "rembrandt/benchmark/kafka/parallel_data_processor.h"

KafkaParallelDataProcessor::KafkaParallelDataProcessor(size_t batch_size,
                                                       tbb::concurrent_bounded_queue<RdKafka::Message *> &received,
                                                       tbb::concurrent_hash_map<uint64_t, uint64_t> &counts,
                                                       size_t num_threads) :
    num_threads_(num_threads),
    counter_(0),
    waiting_(0),
    counts_() {
  for (size_t i = 0; i < num_threads; i++) {
    data_processors_.push_back(std::make_unique<KafkaDataProcessor>(batch_size,
                                                               received,
                                                               counts));
  }
}

void KafkaParallelDataProcessor::Start(size_t batch_count) {
  size_t batches_per_thread = batch_count / num_threads_;
  size_t remainder = batch_count % num_threads_;
  for (size_t i = 0; i < data_processors_.size(); i++) {
    size_t batches = batches_per_thread;
    if (i < remainder) ++batches;
    threads_.push_back(std::thread(&KafkaParallelDataProcessor::StartDataProcessor,
                                   this,
                                   std::ref(*data_processors_[i].get()),
                                   batches));
  }
}

void KafkaParallelDataProcessor::StartDataProcessor(KafkaDataProcessor &data_processor, size_t batch_count) {
  std::unique_lock<std::mutex> lock(mutex_);
  ++counter_;
  ++waiting_;
  condition_variable_.wait(lock, [&] { return counter_.load() >= num_threads_; });
  condition_variable_.notify_one();
  --waiting_;
  if (waiting_.load() == 0) {
    counter_ = 0;
  }
  lock.unlock();
  data_processor.SetRunning();
  data_processor.Run(batch_count);
}

void KafkaParallelDataProcessor::Stop() {
  for (auto const &data_processor: data_processors_) {
    data_processor->Stop();
  }
  for (auto &thread: threads_) {
    thread.join();
  }
}