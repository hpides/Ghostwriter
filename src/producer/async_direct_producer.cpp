#include <rembrandt/producer/async_direct_producer.h>
#include <rembrandt/producer/producer_config.h>
#include <iostream>

AsyncDirectProducer::AsyncDirectProducer(Sender &sender,
                                         ProducerConfig &config) : config_(config),
                                                                   sender_(sender) {}

void AsyncDirectProducer::Start() {
  if (!running_) {
    running_ = true;
    thread_ = std::thread(&AsyncDirectProducer::Run, this);
  } else {
    std::cout << "Producer already running in the background.\n";
  }
}

void AsyncDirectProducer::Stop() {
  if (running_) {
    running_ = false;
    thread_.join();
  } else {
    std::cout << "Producer not running in the background.\n";
  }
}

void AsyncDirectProducer::Run() {
  while (running_) {
    std::unique_ptr<Batch> batch = GetFullBatch();
    sender_.Send(batch.get());
  }
}

void AsyncDirectProducer::Send(TopicPartition topic_partition,
                               std::unique_ptr<Message> message) {
  std::unique_ptr<Batch> batch = std::make_unique<Batch>(topic_partition, std::move(message));
  std::scoped_lock lck{full_batches_mutex};
  full_batches_.push_back(std::move(batch));
  full_batches_cond_.notify_one();
}

std::unique_ptr<Batch> AsyncDirectProducer::GetFullBatch() {
  std::unique_lock lck{full_batches_mutex};
  full_batches_cond_.wait(lck, [&] { return !full_batches_.empty(); });
  std::unique_ptr<Batch> batch = std::move(full_batches_.front());
  full_batches_.pop_front();
  lck.unlock();
  return batch;
}