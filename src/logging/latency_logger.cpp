#include "rembrandt/logging/latency_logger.h"

#include <filesystem>
#include <iomanip>
#include <math.h>
#include <algorithm>
#include <limits>
#include <hdr_histogram.h>
#include <stdio.h>

LatencyLogger::LatencyLogger(long batch_count) : active_(false),
                                                 counter_(0l),
                                                 size_(batch_count),
                                                 histogram_(nullptr) {
  latencies_ = std::vector<long>(size_, 0);
  hdr_init(1, INT64_C(3600000000), 3, &histogram_);
}
void LatencyLogger::Log(long latency) {
  if (active_) {
    latencies_[counter_++] = latency;
    hdr_record_value(histogram_, latency);
//    ++counter_;
  }
//  }
}

void LatencyLogger::Output(std::string dir, std::string prefix) {
  OutputTimeline(dir, prefix);
  OutputHistogram(dir, prefix);
}
void LatencyLogger::OutputHistogram(std::string dir, std::string prefix) {
  std::filesystem::path
      filepath = std::filesystem::path(dir) / std::filesystem::path(prefix + "_latency_histogram.csv");
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
  log_file << "Latency in us\n";
  for (long i = 0; i < size_; ++i) {
    log_file << latencies_[i] << "\n";
  }
  log_file.flush();
  log_file.close();
}
