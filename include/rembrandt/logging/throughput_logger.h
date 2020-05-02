#ifndef REMBRANDT_SRC_LOGGING_THROUGHPUT_LOGGER_H_
#define REMBRANDT_SRC_LOGGING_THROUGHPUT_LOGGER_H_

#include <string>
#include <atomic>
#include <fstream>

#include <boost/asio.hpp>

class ThroughputLogger {
 public:
  ThroughputLogger(std::atomic<long> &counter, std::string dir, std::string filename, int event_size);
  ~ThroughputLogger();
  void Start();
  void Stop();
  void Run();
  void RunOnce(const boost::system::error_code &,
               boost::asio::steady_timer *t);
 private:
  static const int UPDATE_INTERVAL_SECONDS = 1;
  const int event_size_;
  std::atomic<long> &counter_;
  long previous_value_;
  std::ofstream log_file_;
  std::atomic<bool> running_;
  std::thread thread_;
};

#endif //REMBRANDT_SRC_LOGGING_THROUGHPUT_LOGGER_H_
