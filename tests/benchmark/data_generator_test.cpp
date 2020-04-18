#include <chrono>
#include <rembrandt/benchmark/data_generator.h>
#include "gtest/gtest.h"

TEST(DataGeneratorTest, Basic) {
  size_t buffer_size = 100;
  std::unique_ptr<char> buffer((char *) malloc(buffer_size));
  Queue<char *> free(1);
  Queue<char *> generated(1);
  free.push(buffer.get());
  DataGenerator data_generator = DataGenerator(100, free, generated, 0, 1000);
  auto before = std::chrono::steady_clock::now();
  data_generator.GenerateBatch(buffer.get());
  auto after = std::chrono::steady_clock::now();
  long ts = *(long *) buffer.get();
  long before_num = std::chrono::duration_cast<std::chrono::milliseconds>(before.time_since_epoch()).count();
  long after_num = std::chrono::duration_cast<std::chrono::milliseconds>(after.time_since_epoch()).count();
  ASSERT_LE(before_num, ts);
  ASSERT_LE(ts, after_num);
}