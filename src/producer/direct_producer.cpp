#include <rembrandt/producer/direct_producer.h>
#include <rembrandt/producer/producer_config.h>

DirectProducer::DirectProducer(Sender &sender, ProducerConfig &config) : config_(config), sender_(sender) {}

void DirectProducer::Send(TopicPartition topic_partition,
                          void *buffer,
                          size_t length) {
  Batch batch = Batch(topic_partition, (char *) buffer, length, length);
  sender_.Send(&batch);
}
