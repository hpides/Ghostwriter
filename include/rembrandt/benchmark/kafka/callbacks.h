#ifndef REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_YSB_KAFKA_CALLBACKS_H_
#define REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_YSB_KAFKA_CALLBACKS_H_

#include <atomic>
#include <rdkafkacpp.h>
#include <tbb/concurrent_queue.h>


class ThroughputLoggingDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  explicit ThroughputLoggingDeliveryReportCb(std::atomic<long> &counter) :
      counter_(counter) {}
  void dr_cb(RdKafka::Message &message) override {
    if (message.err()) {
      exit(1);
    } else {
      ++counter_;
    }
  }
 private:
  std::atomic<long> &counter_;
};


// class LatencyLoggingDeliveryReportCb : public RdKafka::DeliveryReportCb {
//  public:
//   explicit LatencyLoggingDeliveryReportCb(std::atomic<long> &counter,
//                                           tbb::concurrent_bounded_queue<char *> &free_buffers,
//                                           LatencyLogger &event_latency_logger,
//                                           LatencyLogger &processing_latency_logger) :
//       counter_(counter),
//       free_buffers_(free_buffers),
//       event_latency_logger_(event_latency_logger),
//       processing_latency_logger_(processing_latency_logger) {}
//   void dr_cb(RdKafka::Message &message) override {
//     if (message.err()) {
//       exit(1);
//     } else {
//       auto now = std::chrono::steady_clock::now();
//       long after = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
//       long before_event = *(long *) message.msg_opaque();
//       long before_processing = *(((long *) message.msg_opaque()) + 1);
//       event_latency_logger_.Log(after - before_event);
//       processing_latency_logger_.Log(after - before_processing);
//       ++counter_;
//       free_buffers_.push((char *) message.msg_opaque());
//     }
//   }
//  private:
//   std::atomic<long> &counter_;
//   tbb::concurrent_bounded_queue<char *> &free_buffers_;
//   LatencyLogger &event_latency_logger_;
//   LatencyLogger &processing_latency_logger_;
// };

#endif //REMBRANDT_INCLUDE_REMBRANDT_BENCHMARK_YSB_KAFKA_CALLBACKS_H_
