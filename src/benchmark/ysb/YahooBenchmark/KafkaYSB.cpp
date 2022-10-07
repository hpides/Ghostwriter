#include "rembrandt/benchmark/ysb/YahooBenchmark.h"
#include <tbb/concurrent_queue.h>
#include <rdkafkacpp.h>

class KafkaYSB : public YSB {
 protected:
  tbb::concurrent_bounded_queue<RdKafka::Message *> &received_;
 
  void processOnce(long systemTimestamp) {
    RdKafka::Message *msg;
    received_.pop(msg);
    auto buffer = (char *) msg->payload();
    getApplication()->processData(buffer, batch_size_, systemTimestamp);
    delete msg;
  }
 public:
  KafkaYSB(size_t batch_size,
      tbb::concurrent_bounded_queue<RdKafka::Message *> &received) : YSB(batch_size), received_(received) {
  }
};