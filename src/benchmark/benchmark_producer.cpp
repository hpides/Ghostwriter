#include <rembrandt/network/ucx/context.h>
#include <rembrandt/producer/producer_config.h>
#include <rembrandt/producer/producer.h>
#include <fcntl.h>

int main(int argc, char *argv[]) {
  UCP::Context context = UCP::Context(true);
  ProducerConfig config = ProducerConfig();
  config.send_buffer_size = 1000 * 1000; // 1 MB
  config.max_batch_size = 1000 * 1000; // 1 MB
  Producer producer = Producer(context, config);
  producer.Start();
  TopicPartition topic_partition(1, 1);
  void *buffer = malloc(config.max_batch_size);
  int fd = open("/dev/urandom", O_RDONLY);
  read(fd, buffer, config.max_batch_size);
  for (uint32_t i = 0; i < 1000 * 10; i++) {
    if (i % 100 == 0) {
      printf("Iteration: %d\n", i);
    }
    producer.Send(topic_partition, buffer, config.max_batch_size);
  }
  producer.Stop();
}
