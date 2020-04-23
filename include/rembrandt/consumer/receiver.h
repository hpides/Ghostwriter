#ifndef REMBRANDT_SRC_CONSUMER_RECEIVER_H_
#define REMBRANDT_SRC_CONSUMER_RECEIVER_H_

#include <rembrandt/consumer/consumer_config.h>
#include <rembrandt/network/connection_manager.h>
#include <rembrandt/protocol/message_generator.h>
#include <rembrandt/network/request_processor.h>
#include <rembrandt/network/client.h>

class Receiver : public Client {
 public:
  Receiver(ConnectionManager &connection_manager,
           MessageGenerator &message_generator,
           RequestProcessor &request_processor,
           UCP::Worker &worker,
           ConsumerConfig &config);
  ~Receiver() = default;
  std::unique_ptr<Message> Receive(TopicPartition topic_partition, std::unique_ptr<Message> message, uint64_t offset);
  uint64_t FetchCommittedOffset(TopicPartition topic_partition);
 private:
  ConsumerConfig &config_;
  UCP::Endpoint &GetEndpointWithRKey() const override;
  std::pair<uint64_t, uint32_t> ReceiveFetchedDataLocation(UCP::Endpoint &endpoint);
  uint64_t ReceiveFetchCommittedOffsetResponse(const UCP::Endpoint &endpoint);
};

#endif //REMBRANDT_SRC_CONSUMER_RECEIVER_H_
