#include "rembrandt/benchmark/parallel_data_generator.h"

ParallelDataGenerator::ParallelDataGenerator(size_t batch_size,
                                             tbb::concurrent_bounded_queue<char *> &free,
                                             tbb::concurrent_bounded_queue<char *> &generated,
                                             RateLimiter &rate_limiter,
                                             uint64_t min_key,
                                             uint64_t max_key,
                                             size_t num_threads,
                                             MODE mode) :
    rate_limiter_(rate_limiter),
    num_threads_(num_threads),
    counter_(0),
    waiting_(0) {
  uint64_t keys_per_thread = (max_key - min_key) / num_threads;
  uint64_t leftover_keys = (max_key - min_key) % num_threads;
  uint64_t thread_min_key = min_key;
  uint64_t thread_max_key;
  for (size_t i = 0; i < num_threads; i++) {
    thread_max_key = thread_min_key + keys_per_thread;
    if (i < leftover_keys) {
      thread_max_key += 1;
    }
    data_generators_.push_back(std::make_unique<DataGenerator>(batch_size,
                                                               free,
                                                               generated,
                                                               rate_limiter,
                                                               thread_min_key,
                                                               thread_max_key,
                                                               mode));
    thread_min_key = thread_max_key;
  }
}

void ParallelDataGenerator::Start(size_t batch_count) {
  size_t batches_per_thread = batch_count / num_threads_;
  size_t remainder = batch_count % num_threads_;
  // TODO: Handle leftovers
  for (size_t i = 0; i < data_generators_.size(); i++) {
    size_t batches = batches_per_thread;
    if (i < remainder) ++batches;
    threads_.push_back(std::thread(&ParallelDataGenerator::StartDataGenerator,
                                   this,
                                   std::ref(*data_generators_[i].get()),
                                   batches));
  }
}

void ParallelDataGenerator::StartDataGenerator(DataGenerator &data_generator, size_t batch_count) {
  std::unique_lock<std::mutex> lock(mutex_);
  ++counter_;
  ++waiting_;
  condition_variable_.wait(lock, [&] { return counter_.load() >= num_threads_; });
  condition_variable_.notify_one();
  --waiting_;
  if (waiting_.load() == 0) {
    rate_limiter_.Reset();
    counter_ = 0;
  }
  lock.unlock();
  data_generator.SetRunning();
  data_generator.Run(batch_count);
}

void ParallelDataGenerator::Stop() {
  for (auto const &data_generator: data_generators_) {
    data_generator->Stop();
  }
  for (auto &thread: threads_) {
    thread.join();
  }
}