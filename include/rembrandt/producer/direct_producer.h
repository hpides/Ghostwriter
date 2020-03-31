#ifndef REMBRANDT_SRC_PRODUCER_DIRECT_PRODUCER_H_
#define REMBRANDT_SRC_PRODUCER_DIRECT_PRODUCER_H_

#include <rembrandt/utils.h>
#include <glob.h>
#include "rembrandt/producer/producer_config.h"
#include "producer.h"
#include "sender.h"
class DirectProducer : public Producer {
 public:
  DirectProducer(Sender &sender, ProducerConfig &config);
  virtual void Send(TopicPartition topic_partition, void *buffer, size_t length) override;
 private:
  Sender sender_;
  ProducerConfig config_;
};

#endif //REMBRANDT_SRC_PRODUCER_ASYNC_DIRECT_PRODUCER_H_
