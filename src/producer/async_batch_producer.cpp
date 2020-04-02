#include <rembrandt/producer/async_batch_producer.h>
#include <rembrandt/producer/producer_config.h>

AsyncBatchProducer::AsyncBatchProducer(MessageAccumulator &message_accumulator,
                                       Sender &sender,
                                       ProducerConfig &config) : config_(config),
                                                                 message_accumulator_(message_accumulator),
                                                                 sender_(sender) {}

void AsyncBatchProducer::Start() {
//  sender_.Start();
}

void AsyncBatchProducer::Stop() {
//  sender_.Stop();
}

void AsyncBatchProducer::Send(TopicPartition topic_partition,
                              void *buffer,
                              size_t length) {
  message_accumulator_.Append(topic_partition, (char *) buffer, length);
}
