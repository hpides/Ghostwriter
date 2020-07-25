#ifndef REMBRANDT_SRC_BROKER_BROKER_NODE_H_
#define REMBRANDT_SRC_BROKER_BROKER_NODE_H_

#include <unordered_map>

#include <rembrandt/protocol/flatbuffers/rembrandt_protocol_generated.h>
#include <rembrandt/protocol/message_generator.h>
#include <rembrandt/network/connection_manager.h>
#include <rembrandt/storage/storage_manager.h>
#include "../network/message_handler.h"
#include "../network/server.h"
#include "./broker_node_config.h"
#include "partition.h"
#include "remote_batch.h"

class BrokerNode : public MessageHandler {
 public:
  static constexpr uint64_t STAGED_FLAG = 0ul;
  static constexpr uint64_t COMMIT_FLAG = 1ul;
  static constexpr uint64_t TIMEOUT_FLAG = 2ul;
  BrokerNode(std::unique_ptr<Server> server,
             ConnectionManager &connection_manager,
             std::unique_ptr<MessageGenerator> message_generator,
             RequestProcessor &request_processor,
             std::unique_ptr<UCP::Worker> client_worker,
             BrokerNodeConfig config);
  void Run();
  void AssignPartition(uint32_t topic_id, uint32_t partition_id, Partition::Mode mode);
  std::unique_ptr<Message> HandleMessage(const Message &raw_message) override;
  static uint64_t GetConcurrentMessageSize(uint64_t message_size);
 private:
  BrokerNodeConfig config_;
  ConnectionManager &connection_manager_;
  RequestProcessor &request_processor_;
  std::unique_ptr<UCP::Worker> client_worker_;
  std::unique_ptr<Server> server_;
  std::unordered_map<PartitionIdentifier, std::unique_ptr<Partition>, PartitionIdentifierHash> partitions_;
  std::unique_ptr<Message> HandleCommitRequest(const Rembrandt::Protocol::BaseMessage &commit_request);
  std::unique_ptr<Message> HandleStageRequest(const Rembrandt::Protocol::BaseMessage &stage_request);
  std::unique_ptr<Message> HandleFetchRequest(const Rembrandt::Protocol::BaseMessage &fetch_request);
  LogicalSegment &GetWriteableSegment(uint32_t topic_id, uint32_t partition_id, uint64_t message_size);
  void AllocateSegment(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id, uint64_t start_offset);
  bool Commit(uint32_t topic_id, uint32_t partition_id, uint64_t offset);
  bool ConcurrentCommit(uint32_t topic_id, uint32_t partition_id, uint64_t offset);
  RemoteBatch Stage(uint32_t topic_id,
                    uint32_t partition_id,
                    uint64_t message_size,
                    uint64_t max_batch);
  RemoteBatch ConcurrentStage(uint32_t topic_id,
                              uint32_t partition_id,
                              uint64_t message_size,
                              uint64_t max_batch_size);
  void CloseSegment(LogicalSegment &logical_segment);
  void SendMessage(const Message &message, const UCP::Endpoint &endpoint);
  void WaitUntilReadyToReceive(const UCP::Endpoint &endpoint);
  void ReceiveAllocatedSegment(const UCP::Endpoint &endpoint,
                               uint32_t topic_id,
                               uint32_t partition_id,
                               uint32_t segment_id,
                               uint64_t start_offset);
  Partition &GetPartition(uint32_t topic_id, uint32_t partition_id) const;
};

#endif //REMBRANDT_SRC_BROKER_BROKER_NODE_H_
