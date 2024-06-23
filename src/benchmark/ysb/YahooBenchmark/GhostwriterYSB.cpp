#include "rembrandt/benchmark/ysb/YahooBenchmark.h"
#include <tbb/concurrent_queue.h>

class GhostwriterYSB : public YSB {
 protected:
  tbb::concurrent_bounded_queue<char *> &free_;
  tbb::concurrent_bounded_queue<char *> &received_;
 
  void processOnce(long systemTimestamp) {
    char *buffer;
    received_.pop(buffer);
    getApplication()->processData(buffer, batch_size_, systemTimestamp);
    free_.push(buffer);
  }
 public:
  GhostwriterYSB(size_t batch_size,
      size_t batch_count,
      tbb::concurrent_bounded_queue<char *> &free,
      tbb::concurrent_bounded_queue<char *> &received) : YSB(batch_size, batch_count), free_(free), received_(received) {
  }
int runBenchmark(bool terminate = true) override {
    auto t1 = std::chrono::high_resolution_clock::now();
    char * inputBuffer;
    auto application = getApplication();
    if (SystemConf::getInstance().LATENCY_ON) {
      SystemConf::getInstance().DURATION = m_duration - 3;
    }
    long systemTimestamp = -1;
    std::cout << "Start running " + getApplicationName() + " ..." << std::endl;
    size_t counter = 0;
    try {
      while (true) {
        if (terminate) {
          if (counter == batch_count_) {
            std::cout << "Stop running " + getApplicationName() + " ..." << std::endl;
            return 0;
          }
        }
        if (SystemConf::getInstance().LATENCY_ON) {
          auto currentTime = std::chrono::high_resolution_clock::now();
          auto currentTimeNano =
              std::chrono::duration_cast<std::chrono::nanoseconds>(currentTime.time_since_epoch()).count();
          systemTimestamp = (long) ((currentTimeNano - m_timestampReference) / 1000L);
        }
        processOnce(systemTimestamp);
        counter++;
      }
    } catch (std::exception &e) {
      std::cout << e.what() << std::endl;
      exit(1);
    }
  }
};