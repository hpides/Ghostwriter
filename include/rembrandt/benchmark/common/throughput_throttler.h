#ifndef REMBRANDT_SRC_BENCHMARK_THROUGHPUT_THROTTLER_H_
#define REMBRANDT_SRC_BENCHMARK_THROUGHPUT_THROTTLER_H_

#include <chrono>
#include <cstdint>
#include <condition_variable>
#include <thread>
#include <atomic>

using Clock = std::chrono::steady_clock;
using MilliSeconds = std::chrono::milliseconds;
using NanoSeconds = std::chrono::nanoseconds;
using TimePoint = std::chrono::time_point<Clock>;
using Duration = std::chrono::duration<Clock>;

class ThroughputThrottler {
 public:
  ThroughputThrottler(uint64_t target_throughput_bytes, TimePoint start_time);
  void ThrottleIfNecessary(long bytes_so_far, TimePoint send_start);
 private:
  static constexpr MilliSeconds MIN_SLEEP = MilliSeconds(5);
  uint64_t target_throughput_bytes_;
  TimePoint start_time_;
  Duration sleep_time_;
  NanoSeconds sleep_deficit_;
  std::atomic<bool> wakeup_ = false;
  TimePoint last_time_check_;
  long last_amount_check_ = 0;
  static TimePoint CurrentTime();
  std::mutex mutex_;
  std::condition_variable cond_var_;
};

#endif //REMBRANDT_SRC_BENCHMARK_THROUGHPUT_THROTTLER_H_
