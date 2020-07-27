#include <rembrandt/producer/direct_producer.h>
#include <rembrandt/producer/producer_config.h>

DirectProducer::DirectProducer(Sender &sender, ProducerConfig &config) : sender_(sender), config_(config) {}

void DirectProducer::Send(const TopicPartition &topic_partition, std::unique_ptr<Message> message) {
  uint64_t message_size = message->GetSize();
  Batch batch = Batch(topic_partition, std::move(message), message_size);
  sender_.Send(&batch);
}

void DirectProducer::Send(const TopicPartition &topic_partition, std::unique_ptr<Message> message, uint64_t (&latencies)[4]) {
  uint64_t message_size = message->GetSize();
  Batch batch = Batch(topic_partition, std::move(message), message_size);
  sender_.Send(&batch, latencies);
}
