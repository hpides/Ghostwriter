#include <rembrandt/benchmark/rate_limiter.h>
#include "gtest/gtest.h"

TEST(RateLimiterTest, DefaultConstructor) {
  RateLimiter rate_limiter;
  ASSERT_DOUBLE_EQ(std::micro::den, rate_limiter.GetRate());
}

TEST(RateLimiterTest, Create) {
  RateLimiter rate_limiter = RateLimiter::Create(10);
  ASSERT_DOUBLE_EQ(10, rate_limiter.GetRate());
}

TEST(RateLimiterTest, SetRate) {
  RateLimiter rate_limiter;
  rate_limiter.SetRate(10);
  ASSERT_DOUBLE_EQ(10, rate_limiter.GetRate());
}

TEST(RateLimiter, Limiting) {
  RateLimiter rate_limiter = RateLimiter::Create(10);
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < 21; i++) {
    rate_limiter.Acquire();
  }
  auto end = std::chrono::steady_clock::now();
  auto delta = std::chrono::duration_cast<std::chrono::duration<long, std::centi>>(end - start);
  ASSERT_EQ(200, delta.count());
}