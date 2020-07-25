#include <chrono>
#include <iostream>
#include "rembrandt/benchmark/data_processor.h"

DataProcessor::DataProcessor(size_t batch_size,
                             tbb::concurrent_bounded_queue<char *> &free,
                             tbb::concurrent_bounded_queue<char *> &received,
                             tbb::concurrent_hash_map<uint64_t, uint64_t> &counts) :
    batch_counter_(0),
    batch_size_(batch_size),
    free_(free),
    received_(received),
    local_counts_(1000),
    counts_(counts),
    running_(false) {}

void DataProcessor::Run(size_t batch_count) {
  char *buffer;
  size_t i = 0;
  while (running_ && i < batch_count) {
    buffer = GetReceivedBuffer();
    ProcessBatch(buffer);
    i++;
    free_.push(buffer);
  }
}

void DataProcessor::Stop() {
  if (running_) {
    running_ = false;
  } else {
    std::cout << "DataProcessor not running.\n";
  }
}

void DataProcessor::ProcessBatch(char *buffer) {
  static constexpr size_t element_size = sizeof(uint64_t);
  size_t element_count = (batch_size_ - sizeof(long)) / element_size;
  auto data_location = (uint64_t *) (buffer + sizeof(long));
  for (size_t i = 0; i < element_count; i++) {
    tbb::concurrent_hash_map<uint64_t, uint64_t>::accessor a;
    uint64_t key = *(data_location + i);
    local_counts_.at(key)++;
  }
  batch_counter_++;
}

char *DataProcessor::GetReceivedBuffer() {
  char *buffer;
  received_.pop(buffer);
  return buffer;
}