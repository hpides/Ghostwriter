#ifndef REMBRANDT_SRC_LOGGING_THROUGHPUT_LOGGER_H_
#define REMBRANDT_SRC_LOGGING_THROUGHPUT_LOGGER_H_

#include <string>
#include <atomic>
#include <fstream>

#include <boost/asio.hpp>

class ThroughputLogger {
 public:
  ThroughputLogger(std::atomic<size_t> &counter, std::string dir, std::string filename, int event_size);
  ~ThroughputLogger();
  void Start();
  void Stop();
  void Run();
  void RunOnce();
 private:
  static const int UPDATE_INTERVAL_SECONDS = 1;
  const int event_size_;
  std::atomic<size_t> &counter_;
  long previous_value_;
  std::ofstream log_file_;
  std::atomic<bool> running_;
  std::thread thread_;
};

#endif //REMBRANDT_SRC_LOGGING_THROUGHPUT_LOGGER_H_
