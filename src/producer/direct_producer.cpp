#include <rembrandt/producer/async_direct_producer.h>
#include <rembrandt/producer/producer_config.h>

AsyncDirectProducer::AsyncDirectProducer(MessageAccumulator &message_accumulator,
                                         Sender &sender,
                                         ProducerConfig &config) : config_(config),
                                                                   message_accumulator_(message_accumulator),
                                                                   sender_(sender) {}

void AsyncDirectProducer::Start() {
  sender_.Start();
}

void AsyncDirectProducer::Stop() {
  sender_.Stop();
}

void AsyncDirectProducer::Send(TopicPartition topic_partition,
                               void *buffer,
                               size_t length) {
  message_accumulator_.Append(topic_partition, (char *) buffer, length);
}
