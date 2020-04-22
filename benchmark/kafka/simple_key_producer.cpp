#include <chrono>
#include <iostream>
#include <fcntl.h>
#include <rembrandt/logging/throughput_logger.h>
#include <atomic>
#include <rembrandt/benchmark/data_generator.h>
#include <unordered_set>
#include <rdkafkacpp.h>

class BufferReturnDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  explicit BufferReturnDeliveryReportCb(std::atomic<long> &counter,
                                        tbb::concurrent_bounded_queue<char *> &free_buffers) :
      counter_(counter),
      free_buffers_(free_buffers) {}
  void dr_cb(RdKafka::Message &message) override {
    if (message.err()) {
      exit(1);
    } else {
      ++counter_;
      free_buffers_.push((char *) message.msg_opaque());
    }
  }
 private:
  std::atomic<long> &counter_;
  tbb::concurrent_bounded_queue<char *> &free_buffers_;
};

int main(int argc, char *argv[]) {
  uint send_buffer_size = 131072 * 3;
  uint max_batch_size = 131072;

  const size_t kNumBuffers = 10;
  std::unordered_set<std::unique_ptr<char>> pointers;
  tbb::concurrent_bounded_queue<char *> free_buffers;
  tbb::concurrent_bounded_queue<char *> generated_buffers;
  for (size_t _ = 0; _ < kNumBuffers; _++) {
    std::unique_ptr<char> pointer((char *) malloc(max_batch_size));
    free_buffers.push(pointer.get());
    pointers.insert(std::move(pointer));
  }
  std::atomic<long> counter = 0;
  ThroughputLogger logger = ThroughputLogger(counter, ".", max_batch_size);
  RateLimiter rate_limiter = RateLimiter::Create(10l * 1000 * 1000 * 1000);
  DataGenerator data_generator(max_batch_size, free_buffers, generated_buffers, rate_limiter, 0, 1000, STRICT);

  std::string topic = "TestTopic";

  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  std::string errstr;
  if (conf->set("bootstrap.servers", "10.10.0.11", errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  BufferReturnDeliveryReportCb br_dr_cb(counter, free_buffers);
  if (conf->set("dr_cb", &br_dr_cb, errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
  if (!producer) {
    std::cerr << "Failed to create producer: " << errstr << std::endl;
    exit(1);
  }

  delete conf;

  const size_t batch_count = 1000l * 1000 * 1000 * 100 / max_batch_size;
  data_generator.Start(batch_count);
  logger.Start();
  auto start = std::chrono::high_resolution_clock::now();
  char *buffer;
  for (long count = 0; count < batch_count; count++) {
    if (count % 100000 == 0) {
      printf("Iteration: %d\n", count);
    }
    generated_buffers.pop(buffer);
    producer->produce(topic,
                      0,
                      0,
                      buffer,
                      send_buffer_size,
                      nullptr,
                      0,
                      0,
                      nullptr,
                      buffer);
    producer->poll(10);
    free_buffers.push(buffer);
    counter++;
  }

  auto stop = std::chrono::high_resolution_clock::now();
  logger.Stop();
  data_generator.Stop();

  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";
}
