#include <memory>
#include <cstring>
#include <iostream>
#include <boost/program_options.hpp>
#include <rembrandt/benchmark/common/data_generator.h>
#include <rembrandt/consumer/direct_consumer.h>
#include <rembrandt/producer/direct_producer.h>
#include <rembrandt/network/ucx/context.h>
#include <rembrandt/network/attached_message.h>
#include <rembrandt/protocol/protocol.h>

class RoundTripTest{
    public:
        RoundTripTest(int argc, char *const *argv);
        void Run();

    private:
        void ParseOptions(int argc, char *const *argv);
        ProducerConfig producer_config_;
        ConsumerConfig consumer_config_;
        std::unique_ptr<UCP::Context> context_p_;
        std::unique_ptr<Producer> producer_p_;
        std::unique_ptr<Consumer> consumer_p_;
        std::unique_ptr<DataGenerator> generator_p_;
        size_t GetBatchCount();
        size_t GetEffectiveBatchSize();
};

RoundTripTest::RoundTripTest(int argc, char *const *argv) 
        : context_p_(std::make_unique<UCP::Context>(true)) {
    this->ParseOptions(argc, argv);
    producer_p_ = DirectProducer::Create(producer_config_, *context_p_);
    consumer_p_ = DirectConsumer::Create(consumer_config_, *context_p_);
    generator_p_ = std::make_unique<DataGenerator>(producer_config_.max_batch_size, 0, 1000, MODE::RELAXED);
}


void RoundTripTest::Run() {
    std::cout << "Starting..." << std::endl;
    auto send_buffer = (char *) malloc(GetEffectiveBatchSize());
    auto recv_buffer = (char *) malloc(GetEffectiveBatchSize()); // TODO: CHECK
    for (size_t count = 0; count < GetBatchCount(); count++) {
        if (count % (GetBatchCount() / 10) == 0) {
            std::cout << "Iteration: " << count << std::endl;
        }
        memset(send_buffer, 0, GetEffectiveBatchSize());
        memset(recv_buffer, 0, GetEffectiveBatchSize());
        generator_p_->GenerateBatch(send_buffer);
        auto send_message = std::make_unique<AttachedMessage>(send_buffer, GetEffectiveBatchSize());
        producer_p_->Send(1, 1, std::move(send_message));
        
        auto recv_message = std::make_unique<AttachedMessage>(recv_buffer, GetEffectiveBatchSize());
        consumer_p_->Receive(1, 1, std::move(recv_message));
        if (memcmp(send_buffer, recv_buffer, GetEffectiveBatchSize()) != 0) {
            std::cout << "Mismatch in iteration " << count << std::endl;
        }
    } 
    std::cout << "Finished." << std::endl;
}

void RoundTripTest::ParseOptions(int argc, char *const *argv) {
  namespace po = boost::program_options;
  std::string mode_str;
  try {
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("broker-node-ip",
         po::value(&producer_config_.broker_node_ip)->default_value("10.150.1.12"),
         "IP address of the broker node")
        ("broker-node-port",
         po::value(&producer_config_.broker_node_port)->default_value(13360),
         "Port number of the broker node")
        ("storage-node-ip",
         po::value(&producer_config_.storage_node_ip)->default_value("10.150.1.12"),
         "IP address of the storage node")
        ("storage-node-port",
         po::value(&producer_config_.storage_node_port)->default_value(13350),
         "Port number of the storage node")
        ("batch-size",
         po::value(&producer_config_.max_batch_size)->default_value(131072),
         "Maximum size of an individual batch (sending unit) in bytes")
        ("data-size", po::value(&producer_config_.data_size)->default_value(producer_config_.data_size),
         "Total amount of data transferred in this benchmark")
        ("mode", po::value(&mode_str), "The mode in which the test is run, 'exclusive' or 'concurrent'");

    po::variables_map variables_map;
    po::store(po::parse_command_line(argc, argv, desc), variables_map);
    po::notify(variables_map);

    if (variables_map.count("help")) {
      std::cout << "Usage: myExecutable [options]\n";
      std::cout << desc;
      exit(0);
    }
    if (mode_str == "exclusive") {
      producer_config_.mode = Partition::Mode::EXCLUSIVE;
    } else if (mode_str == "concurrent") {
      producer_config_.mode = Partition::Mode::CONCURRENT;
    } else {
      std::cout << "Could not parse mode: '" << mode_str << "'" << std::endl;
      exit(1);
    }
    consumer_config_.broker_node_ip = producer_config_.broker_node_ip;
    consumer_config_.broker_node_port = producer_config_.broker_node_port;
    consumer_config_.storage_node_ip = producer_config_.storage_node_ip;
    consumer_config_.storage_node_port = producer_config_.storage_node_port;
    consumer_config_.max_batch_size = producer_config_.max_batch_size;
    consumer_config_.data_size = producer_config_.data_size;
    consumer_config_.mode = producer_config_.mode;

  } catch (const po::error &ex) {
    std::cout << ex.what() << std::endl;
    exit(1);
  }
}

size_t RoundTripTest::GetBatchCount() {
  return producer_config_.data_size / producer_config_.max_batch_size;
}

size_t RoundTripTest::GetEffectiveBatchSize() {
    return Protocol::GetEffectiveBatchSize(producer_config_.max_batch_size, producer_config_.mode);
}


int main(int argc, char *argv[]) {
    RoundTripTest round_trip_test(argc, argv);
    round_trip_test.Run();
}
