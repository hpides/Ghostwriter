#include <rembrandt/producer/producer.h>
#include <rembrandt/producer/producer_config.h>

Producer::Producer(UCP::Context &context, ProducerConfig config)
    : client_(context),
      config_(config),
      message_accumulator_(config.send_buffer_size, config.max_batch_size),
      sender_(client_, message_accumulator_) {}

void Producer::Start() {// TODO: Configure inputs
  /* Initialize the UCX required objects */
  UCP::Endpoint &ep =
      client_.GetConnection(config_.storage_node_ip, config_.storage_node_port);
  ucp_rkey_h rkey = client_.RegisterRemoteMemory(ep,
                                                 config_.storage_node_ip,
                                                 config_.storage_node_rkey_port);
  printf("%p", rkey);
  sender_.Start(ep);
}

void Producer::Stop() {
  sender_.Stop();
}

void Producer::Send(TopicPartition topic_partition,
                    void *buffer,
                    size_t length) {
  message_accumulator_.Append(topic_partition, (char *) buffer, length);
}
