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
      tbb::concurrent_bounded_queue<char *> &free,
      tbb::concurrent_bounded_queue<char *> &received) : YSB(batch_size), free_(free), received_(received) {
    m_name = "YSB";
    createSchema();
    loadInMemoryData();
    createApplication();
  }
};