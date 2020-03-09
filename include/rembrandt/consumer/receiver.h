#ifndef REMBRANDT_SRC_CONSUMER_RECEIVER_H_
#define REMBRANDT_SRC_CONSUMER_RECEIVER_H_

#include <rembrandt/consumer/consumer_config.h>
#include <rembrandt/network/connection_manager.h>
#include <rembrandt/protocol/message_generator.h>
#include <rembrandt/network/request_processor.h>
class Receiver {
 public:
  Receiver(ConnectionManager &connection_manager,
           MessageGenerator &message_generator,
           RequestProcessor &request_processor,
           ConsumerConfig &config);
 public:
  void *Next(TopicPartition topic_partition);
 private:
  ConnectionManager &connection_manager_;
  MessageGenerator &message_generator_;
  RequestProcessor &request_processor_;
  ConsumerConfig &config_;
  std::unordered_map<TopicPartition,
                     uint64_t,
                     boost::hash<TopicPartition>> read_offsets_;
  void SendMessage(Message &message, UCP::Endpoint &endpoint);
  UCP::Endpoint &GetEndpointWithRKey() const;
  std::pair<uint64_t, uint32_t> ReceiveFetchedDataLocation(UCP::Endpoint &endpoint);
};

#endif //REMBRANDT_SRC_CONSUMER_RECEIVER_H_
