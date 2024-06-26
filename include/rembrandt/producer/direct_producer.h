#ifndef REMBRANDT_SRC_PRODUCER_DIRECT_PRODUCER_H_
#define REMBRANDT_SRC_PRODUCER_DIRECT_PRODUCER_H_

#include "producer.h"
#include "rembrandt/producer/producer_config.h"
#include "sender.h"
#include <glob.h>
#include <rembrandt/utils.h>
class DirectProducer : public Producer {
public:
  static std::unique_ptr<DirectProducer> Create(ProducerConfig config, UCP::Context &context);
  void Send(uint32_t topic_id, uint32_t partition_id,
            std::unique_ptr<Message> message) override;
  void Send(uint32_t topic_id, uint32_t partition_id,
            std::unique_ptr<Message> message,
            uint64_t (&latencies)[4]) override;

private:
  DirectProducer(std::unique_ptr<Sender> sender_p, ProducerConfig config);
  std::unique_ptr<Sender> sender_p_;
  ProducerConfig config_;
};

#endif // REMBRANDT_SRC_PRODUCER_ASYNC_DIRECT_PRODUCER_H_
