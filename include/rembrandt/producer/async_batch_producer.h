#ifndef REMBRANDT_SRC_PRODUCER_ASYNC_BATCH_PRODUCER_H_
#define REMBRANDT_SRC_PRODUCER_ASYNC_BATCH_PRODUCER_H_

#include "rembrandt/producer/message_accumulator.h"
#include "rembrandt/producer/producer_config.h"
#include "rembrandt/producer/sender.h"
#include "producer.h"
class AsyncBatchProducer : public Producer {
 public:
  AsyncBatchProducer(MessageAccumulator &message_accumulator, Sender &sender, ProducerConfig &config);
  virtual void Send(TopicPartition topic_partition, std::unique_ptr<Message>) override;
  void Start();
  void Stop();
 private:
  ProducerConfig &config_;
  MessageAccumulator &message_accumulator_;
  Sender &sender_;
};

#endif //REMBRANDT_SRC_PRODUCER_ASYNC_BATCH_PRODUCER_H_
