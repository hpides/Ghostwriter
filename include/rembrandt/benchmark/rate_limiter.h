#ifndef REMBRANDT_SRC_BENCHMARK_RATE_LIMITER_H_
#define REMBRANDT_SRC_BENCHMARK_RATE_LIMITER_H_

#include <chrono>
#include <mutex>
#include <memory>

using Clock = std::chrono::steady_clock;
using Microseconds = std::chrono::microseconds;
using MicrosecondFractions = std::chrono::duration<double, Microseconds::period>;
using Timepoint = std::chrono::time_point<Clock>;

class RateLimiter {
 public:
  RateLimiter();
  std::unique_ptr<RateLimiter> static Create(double permits_per_second);
  ~RateLimiter() = default;
  RateLimiter(const RateLimiter &other);
  RateLimiter(RateLimiter &&other) = delete;
  RateLimiter &operator=(const RateLimiter &other) = delete;
  RateLimiter &operator=(RateLimiter &&other) = delete;
  void Acquire();
  void Acquire(int permits);
  double GetRate();
  void SetRate(double permits_per_second);
  void Reset();
 private:
  double stored_permits_;
  MicrosecondFractions stable_interval_;
  Timepoint next_free_ticket_;
  mutable std::mutex mutex_;
  RateLimiter(const RateLimiter &other, const std::lock_guard<std::mutex> &);
  Microseconds Reserve(int permits);
  void Resync(std::chrono::time_point<Clock> now);
};

#endif //REMBRANDT_SRC_BENCHMARK_RATE_LIMITER_H_
