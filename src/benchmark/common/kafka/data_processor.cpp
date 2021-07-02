#include <chrono>
#include <iostream>
#include <string>
#include "rembrandt/benchmark/kafka/data_processor.h"

KafkaDataProcessor::KafkaDataProcessor(size_t batch_size,
                             tbb::concurrent_bounded_queue<RdKafka::Message *> &received,
                             tbb::concurrent_hash_map<uint64_t, uint64_t> &counts) :
    batch_counter_(0),
    batch_size_(batch_size),
    received_(received),
    local_counts_(1000),
    counts_(counts),
    running_(false) {}

void KafkaDataProcessor::Run(size_t batch_count) {
  char *buffer;
  size_t i = 0;
  while (running_ && i < batch_count) {
    RdKafka::Message *msg = nullptr;
    received_.pop(msg);
    buffer = (char *) msg->payload();
    ProcessBatch(buffer);
    delete msg;
    i++;
  }
}

void KafkaDataProcessor::Stop() {
  if (running_) {
    running_ = false;
  } else {
    std::cout << "DataProcessor not running.\n";
  }
}

void KafkaDataProcessor::ProcessBatch(char *buffer) {
  static constexpr size_t element_size = sizeof(uint64_t);
  size_t element_count = (batch_size_ - (sizeof(long) * 2)) / element_size;
  auto data_location = (uint64_t *) (buffer + (sizeof(long) * 2));
  for (size_t i = 0; i < element_count; i++) {
    tbb::concurrent_hash_map<uint64_t, uint64_t>::accessor a;
    uint64_t key = *(data_location + i);
    local_counts_.at(key)++;
  }
  ++batch_counter_;
}
