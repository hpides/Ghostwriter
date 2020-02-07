#ifndef REMBRANDT_SRC_PRODUCER_PRODUCER_H_
#define REMBRANDT_SRC_PRODUCER_PRODUCER_H_

#include "rembrandt/producer/message_accumulator.h"
#include "rembrandt/producer/producer_config.h"
#include "rembrandt/producer/sender.h"
#include "rembrandt/network/ucx/client.h"
class Producer {
 public:
  Producer(UCP::Context &context, ProducerConfig config);
  void Send(TopicPartition topic_partition, void *buffer, size_t length);
  void Start();
  void Stop();
 private:
  UCP::Client client_;
  ProducerConfig config_;
  MessageAccumulator message_accumulator_;
  Sender sender_;
};

#endif //REMBRANDT_SRC_PRODUCER_PRODUCER_H_
