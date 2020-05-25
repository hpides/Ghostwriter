#include "rembrandt/logging/latency_logger.h"

#include <filesystem>
#include <iomanip>
#include <math.h>
#include <algorithm>
#include <limits>
#include <hdr_histogram.h>
#include <stdio.h>

LatencyLogger::LatencyLogger(long batch_count, long window_size) : counter_(0l), window_size_(window_size), histogram_() {
  window_count_ = ceil(1.0 * batch_count / window_size);
  avg_latencies_ = std::vector<long double>(window_count_, 0.0);
  max_latencies_ = std::vector<long>(window_count_, 0);
  min_latencies_ = std::vector<long>(window_count_, std::numeric_limits<long>::max());
  hdr_init(1, INT64_C(3600000000), 3, &histogram_);
}
void LatencyLogger::Log(long latency) {
  long window = counter_ / window_size_;
  max_latencies_[window] = std::max(max_latencies_[window], latency);
  min_latencies_[window] = std::min(min_latencies_[window], latency);
  long in_window_index = (counter_ % window_size_) + 1;
  avg_latencies_[window] += ((long double) latency - avg_latencies_[window]) / in_window_index;
//  if (count > (batch_count / 100 * 5)) {
  hdr_record_value(histogram_, latency);
  ++counter_;
//  }
}

void LatencyLogger::Output(std::string dir, std::string prefix) {
  OutputTimeline(dir, prefix);
  OutputHistogram(dir, prefix);
}
void LatencyLogger::OutputHistogram(std::string dir, std::string prefix) {
  std::filesystem::path filepath = std::filesystem::path(dir) / std::filesystem::path(prefix + "_latency_histogram.csv");
  std::string filestring = filepath.generic_string();
  FILE *log_file;
  log_file = fopen(filestring.c_str(), "w");
  if (!log_file) {
    throw std::runtime_error("Unable to open file '" + filestring + "'!\n");
  }
  hdr_percentiles_print(histogram_, log_file, 5, 1.0, CLASSIC);
  fflush(log_file);
  fclose(log_file);
}

void LatencyLogger::OutputTimeline(std::string dir, std::string prefix) {
  std::filesystem::path filepath = std::filesystem::path(dir) / std::filesystem::path(prefix + "_latency_timeline.csv");
  std::string filestring = filepath.generic_string();
  std::ofstream log_file;
  log_file.open(filepath, std::ofstream::out | std::ofstream::trunc);
  if (!log_file) {
    throw std::runtime_error("Unable to open file '" + filestring + "'!\n");
  }
  log_file << "// # of Total Messages: " << counter_ << "\n";
  log_file << "// # Messages per Window: " << window_size_ << "\n";
  log_file << "Avg. Latency in us\tMin. Latency in us\tMax. Latency in us\n";
  for (long i = 0; i < window_count_; ++i) {
    log_file << std::fixed << std::setprecision(1) << avg_latencies_[i] << "\t" << min_latencies_[i] << "\t"
             << max_latencies_[i] << "\n";
  }
  log_file.flush();
  log_file.close();
}
