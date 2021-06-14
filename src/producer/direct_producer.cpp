#include <rembrandt/producer/direct_producer.h>
#include <rembrandt/producer/producer_config.h>

DirectProducer::DirectProducer(std::unique_ptr<Sender> sender_p,
                               ProducerConfig config)
    : sender_p_(std::move(sender_p)), config_(config) {}

void DirectProducer::Send(const TopicPartition &topic_partition,
                          std::unique_ptr<Message> message) {
  uint64_t message_size = message->GetSize();
  Batch batch = Batch(topic_partition, std::move(message), message_size);
  sender_p_->Send(&batch);
}

void DirectProducer::Send(const TopicPartition &topic_partition,
                          std::unique_ptr<Message> message,
                          uint64_t (&latencies)[4]) {
  uint64_t message_size = message->GetSize();
  Batch batch = Batch(topic_partition, std::move(message), message_size);
  sender_p_->Send(&batch, latencies);
}

DirectProducer DirectProducer::Create(ProducerConfig config,
                                      UCP::Context &context) {
  std::unique_ptr<MessageGenerator> message_generator_p;
  std::unique_ptr<UCP::Worker> worker_p = context.CreateWorker();
  std::unique_ptr<UCP::EndpointFactory> endpoint_factory_p;
  std::unique_ptr<RequestProcessor> request_processor_p = std::make_unique<RequestProcessor>(*worker_p);
  std::unique_ptr<ConnectionManager>
      connection_manager_p = std::make_unique<ConnectionManager>(std::move(endpoint_factory_p),
                                                                 *worker_p,
                                                                 *message_generator_p,
                                                                 *request_processor_p);
  std::unique_ptr<Sender> sender_p = std::make_unique<Sender>(std::move(connection_manager_p),
                                                              std::move(message_generator_p),
                                                              std::move(request_processor_p),
                                                              std::move(worker_p),
                                                              config);
  return DirectProducer(std::move(sender_p), config);
}