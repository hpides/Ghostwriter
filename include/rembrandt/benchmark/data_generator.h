#ifndef REMBRANDT_SRC_BENCHMARK_DATA_GENERATOR_H_
#define REMBRANDT_SRC_BENCHMARK_DATA_GENERATOR_H_

#include <boost/lockfree/queue.hpp>
template<typename T>
using Queue = boost::lockfree::queue<T>;
class DataGenerator {
 public:
  DataGenerator(size_t batch_size,
                Queue<char *> &free,
                Queue<char *> &generated,
                uint64_t min_key,
                uint64_t max_key);
  void GenerateBatch(char *buffer);
  void Run();
 private:
  size_t batch_counter_;
  const size_t batch_size_;
  static const size_t benchmark_count_ = 100;
  Queue<char *> &free_;
  Queue<char *> &generated_;
  const uint64_t min_key_;
  const uint64_t max_key_;
  char *GetFreeBuffer();
};

#endif //REMBRANDT_SRC_BENCHMARK_DATA_GENERATOR_H_
