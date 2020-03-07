#include <rembrandt/producer/producer.h>
#include <rembrandt/producer/producer_config.h>

Producer::Producer(MessageAccumulator &message_accumulator, Sender &sender, ProducerConfig &config) :
    config_(config),
    message_accumulator_(message_accumulator),
    sender_(sender) {}

void Producer::Start() {
  sender_.Start();
}

void Producer::Stop() {
  sender_.Stop();
}

void Producer::Send(TopicPartition topic_partition,
                    void *buffer,
                    size_t length) {
  message_accumulator_.Append(topic_partition, (char *) buffer, length);
}
