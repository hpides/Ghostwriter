#include <rembrandt/producer/direct_producer.h>
#include <rembrandt/producer/producer_config.h>

DirectProducer::DirectProducer(Sender &sender, ProducerConfig &config) : config_(config), sender_(sender) {}

void DirectProducer::Send(const TopicPartition &topic_partition, std::unique_ptr<Message> message) {
  uint64_t message_size = message->GetSize();
  Batch batch = Batch(topic_partition, std::move(message), message_size);
  sender_.Send(&batch);
}
