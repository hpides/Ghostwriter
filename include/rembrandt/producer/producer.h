#ifndef REMBRANDT_SRC_PRODUCER_PRODUCER_H_
#define REMBRANDT_SRC_PRODUCER_PRODUCER_H_

#include "rembrandt/producer/message_accumulator.h"
#include "rembrandt/producer/producer_config.h"
#include "rembrandt/producer/sender.h"
class Producer {
 public:
  Producer(MessageAccumulator &message_accumulator, Sender &sender, ProducerConfig &config);
  void Send(TopicPartition topic_partition, void *buffer, size_t length);
  void Start();
  void Stop();
 private:
  ProducerConfig &config_;
  MessageAccumulator &message_accumulator_;
  Sender &sender_;
};

#endif //REMBRANDT_SRC_PRODUCER_PRODUCER_H_
