#ifndef REMBRANDT_SRC_ACCUMULATING_PRODUCER_PRODUCER_H_
#define REMBRANDT_SRC_ACCUMULATING_PRODUCER_PRODUCER_H_

#include "rembrandt/producer/message_accumulator.h"
#include "rembrandt/producer/producer_config.h"
#include "rembrandt/producer/sender.h"
class AccumulatingProducer : public Producer {
 public:
  AccumulatingProducer(MessageAccumulator &message_accumulator, Sender &sender, ProducerConfig &config);
  void Send(TopicPartition topic_partition, void *buffer, size_t length) override;
  void Start();
  void Stop();
 private:
  ProducerConfig &config_;
  MessageAccumulator &message_accumulator_;
  Sender &sender_;
};

#endif //REMBRANDT_SRC_ACCUMULATING_PRODUCER_PRODUCER_H_
