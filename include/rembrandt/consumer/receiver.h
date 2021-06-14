#ifndef REMBRANDT_SRC_CONSUMER_RECEIVER_H_
#define REMBRANDT_SRC_CONSUMER_RECEIVER_H_

#include <rembrandt/consumer/consumer_config.h>
#include <rembrandt/network/connection_manager.h>
#include <rembrandt/protocol/message_generator.h>
#include <rembrandt/network/request_processor.h>
#include <rembrandt/network/client.h>
#include "consumer_segment_info.h"
#include "read_segment.h"

struct FetchedData {
  uint64_t start_offset;
  uint64_t commit_offset;
  bool is_committable;
};

class Receiver : public Client {
 public:
  Receiver(std::unique_ptr<ConnectionManager> connection_manager_p,
           std::unique_ptr<MessageGenerator> message_generator_p,
           std::unique_ptr<RequestProcessor> request_processor_p,
           std::unique_ptr<UCP::Worker> worker_p,
           ConsumerConfig &config);
  ~Receiver() = default;
  std::unique_ptr<Message> Receive(std::unique_ptr<Message> message, uint64_t offset);
  std::unique_ptr<ReadSegment> Fetch(uint32_t topic_id, uint32_t partition_id, uint64_t logical_offset);
 private:
  ConsumerConfig &config_;
  UCP::Endpoint &GetEndpointWithRKey() const override;
};

#endif //REMBRANDT_SRC_CONSUMER_RECEIVER_H_
