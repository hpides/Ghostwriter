#include <rembrandt/producer/direct_producer.h>
#include <rembrandt/producer/producer_config.h>

DirectProducer::DirectProducer(Sender &sender, ProducerConfig &config) : config_(config), sender_(sender) {}

void DirectProducer::Send(const TopicPartition &topic_partition, std::unique_ptr<Message> message) {
  Batch batch = Batch(topic_partition, std::move(message), message->GetSize());
  sender_.Send(&batch);
}
