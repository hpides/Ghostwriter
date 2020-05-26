#ifndef REMBRANDT_SRC_LOGGING_LATENCY_LOGGER_H_
#define REMBRANDT_SRC_LOGGING_LATENCY_LOGGER_H_

#include <string>
#include <atomic>
#include <fstream>

#include <boost/asio.hpp>

class LatencyLogger {
 public:
  LatencyLogger(long batch_count, long window_size);
  void Activate() { active_ = true; };
  void Log(long latency);
  void Output(std::string dir, std::string prefix);
 private:
  std::atomic<bool> active_;
  long counter_;
  long window_count_;
  long window_size_;
  std::vector<long double> avg_latencies_;
  std::vector<long> max_latencies_;
  std::vector<long> min_latencies_;
  struct hdr_histogram *histogram_;
  void OutputTimeline(std::string dir, std::string prefix);
  void OutputHistogram(std::string dir, std::string prefix);
};

#endif //REMBRANDT_SRC_LOGGING_LATENCY_LOGGER_H_
