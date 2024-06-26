#ifndef REMBRANDT_SRC_LOGGING_LATENCY_LOGGER_H_
#define REMBRANDT_SRC_LOGGING_LATENCY_LOGGER_H_

#include <string>
#include <atomic>
#include <fstream>

#include <boost/asio.hpp>

class LatencyLogger {
 public:
  LatencyLogger(long batch_count);
  void Activate() { active_ = true; };
  void Log(long latency);
  void Output(std::string dir, std::string prefix);
 private:
  std::atomic<bool> active_;
  long counter_;
  long size_;
  std::vector<long> latencies_;
  struct hdr_histogram *histogram_;
  void OutputTimeline(std::string dir, std::string prefix);
  void OutputHistogram(std::string dir, std::string prefix);
};

#endif //REMBRANDT_SRC_LOGGING_LATENCY_LOGGER_H_
