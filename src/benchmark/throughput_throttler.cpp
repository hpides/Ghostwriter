#include "rembrandt/benchmark/throughput_throttler.h"

ThroughputThrottler::ThroughputThrottler(uint64_t target_throughput_bytes, TimePoint start_time) :
    target_throughput_bytes_(target_throughput_bytes),
    start_time_(start_time) {}

void ThroughputThrottler::ThrottleIfNecessary(long bytes_so_far, TimePoint send_start) {
  auto elapsed_time = send_start - start_time_;
  if (elapsed_time <= std::chrono::duration_values<MilliSeconds>::zero()) {
    return;
  }
  long double current_throughput = (long double) bytes_so_far / std::chrono::duration<float>(elapsed_time).count();
  if (current_throughput <= target_throughput_bytes_) {
    return;
  }
  auto interval = CurrentTime() - last_time_check_;
  auto planned_duration = NanoSeconds((long) (current_throughput / target_throughput_bytes_ * std::nano().den));
  sleep_deficit_ += (planned_duration - interval);

  // If enough deficit, sleep a little
  if (sleep_deficit_ >= MIN_SLEEP) {
    auto sleep_start_ = CurrentTime();
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cond_var_.wait(lock, [&] { return wakeup_.load(); });
      wakeup_ = false;
      NanoSeconds remaining = sleep_deficit_;
      while (remaining > std::chrono::duration_values<NanoSeconds>::zero()) {
        std::this_thread::sleep_for(remaining);
        auto elapsed = CurrentTime() - sleep_start_;
        remaining = sleep_deficit_ - elapsed;
      }
      wakeup_ = true;
      cond_var_.notify_one();
    }
    last_time_check_ = CurrentTime();
    last_amount_check_ = bytes_so_far;
  }

}

TimePoint ThroughputThrottler::CurrentTime() {
  return Clock::now();
}
