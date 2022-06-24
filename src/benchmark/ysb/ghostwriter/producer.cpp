#include <iostream>
#include <boost/program_options.hpp>
#include <rembrandt/benchmark/ysb/ghostwriter/producer.h>
#include <rembrandt/broker/broker_node.h>
#include <rembrandt/logging/throughput_logger.h>
#include <rembrandt/network/attached_message.h>
#include <rembrandt/protocol/protocol.h>

YSBGhostwriterProducer::YSBGhostwriterProducer(int argc, char *const *argv)
    : context_p_(std::make_unique<UCP::Context>(true)) {
  this->ParseOptions(argc, argv);

  producer_p_ = DirectProducer::Create(config_, *context_p_);

  ReadIntoMemory();
}

void YSBGhostwriterProducer::ReadIntoMemory() {
  FILE *f = fopen(input_path_.c_str(), "rb");
  fseek(f, 0, SEEK_END);
  fsize_ = ftell(f);
  // TODO: Assert that fsize >= data_size
  fseek(f, 0, SEEK_SET);  /* same as rewind(f); */

  input_p_ = (char *) malloc(fsize_ + 1);
  fread(input_p_, 1, fsize_, f);
  fclose(f);

  input_p_[fsize_] = 0;
}
//void YSBGhostwriterProducer::Warmup() {
//// TODO
//}

void YSBGhostwriterProducer::Run() {
//  Warmup();
  std::cout << "Starting logger..." << std::endl;
  std::atomic<long> counter = 0;
  ThroughputLogger logger =
      ThroughputLogger(counter, config_.log_directory, "benchmark_producer_throughput", config_.max_batch_size);
  logger.Start();
  std::cout << "Preparing run..." << std::endl;

  auto start = std::chrono::high_resolution_clock::now();

  char *buffer;

  std::cout << "Starting run execution..." << std::endl;
  long numBatchesInFile = fsize_ / GetBatchSize();
  for (size_t count = 0; count < GetRunBatchCount(); count++) {
    if (count % (GetRunBatchCount() / 10) == 0) {
      std::cout << "Iteration: " << count << std::endl;
    }
// TODO
    producer_p_->Send(1, 1, std::make_unique<AttachedMessage>(input_p_ + (GetBatchSize() * (count % numBatchesInFile)), GetBatchSize()));
    ++counter;
  }
  std::cout << "Finishing run execution..." << std::endl;
  auto stop = std::chrono::high_resolution_clock::now();
  logger.Stop();
  std::cout << "Finished logger." << std::endl;
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";
}

void YSBGhostwriterProducer::ParseOptions(int argc, char *const *argv) {
  namespace po = boost::program_options;
  std::string mode_str;
  try {
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("broker-node-ip",
         po::value(&config_.broker_node_ip)->default_value("10.150.1.12"),
         "IP address of the broker node")
        ("broker-node-port",
         po::value(&config_.broker_node_port)->default_value(13360),
         "Port number of the broker node")
        ("storage-node-ip",
         po::value(&config_.storage_node_ip)->default_value("10.150.1.12"),
         "IP address of the storage node")
        ("storage-node-port",
         po::value(&config_.storage_node_port)->default_value(13350),
         "Port number of the storage node")
        ("batch-size",
         po::value(&config_.max_batch_size)->default_value(131072),
         "Size of an individual batch (sending unit) in bytes")
        ("data-size", po::value(&config_.data_size)->default_value(config_.data_size),
         "Total amount of data transferred in this benchmark")
        ("warmup-fraction", po::value(&config_.warmup_fraction)->default_value(config_.warmup_fraction),
         "Fraction of data that is transferred during warmup")
        ("rate-limit", po::value(&config_.rate_limit)->default_value(config_.rate_limit),
         "The maximum amount of data that is transferred per second")
        ("input",
         po::value(&input_path_)->default_value(
             "/hpi/fs00/home/hendrik.makait/ghostwriter/ysb1B0.bin"),
         "File to load generated YSB data")
        ("log-dir",
         po::value(&config_.log_directory)->default_value(
             "/hpi/fs00/home/hendrik.makait/rembrandt/logs/20200727/e2e/50/exclusive_opt/"),
         "Directory to store benchmark logs")
        ("mode", po::value(&mode_str), "The mode in which the producer is run, 'exclusive' or 'concurrent'");

    po::variables_map variables_map;
    po::store(po::parse_command_line(argc, argv, desc), variables_map);
    po::notify(variables_map);

    if (variables_map.count("help")) {
      std::cout << "Usage: myExecutable [options]\n";
      std::cout << desc;
      exit(0);
    }
    if (mode_str == "exclusive") {
      config_.mode = Partition::Mode::EXCLUSIVE;
    } else if (mode_str == "concurrent") {
      config_.mode = Partition::Mode::CONCURRENT;
    } else {
     std::cout << "Could not parse mode: '" << mode_str <<"'" << std::endl;
     exit(1);
    }
  } catch (const po::error &ex) {
    std::cout << ex.what() << std::endl;
    exit(1);
  }
}

size_t YSBGhostwriterProducer::GetBatchCount() {
  return config_.data_size / GetBatchSize();
}

size_t YSBGhostwriterProducer::GetRunBatchCount() {
  return GetBatchCount() - GetWarmupBatchCount();
}
size_t YSBGhostwriterProducer::GetWarmupBatchCount() {
  return GetBatchCount() * config_.warmup_fraction;
}

size_t YSBGhostwriterProducer::GetBatchSize() {
  return (config_.max_batch_size / 128) * 128;
}

size_t YSBGhostwriterProducer::GetEffectiveBatchSize() {
  return Protocol::GetEffectiveBatchSize(GetBatchSize(), config_.mode);
}

int main(int argc, char *argv[]) {
  YSBGhostwriterProducer producer(argc, argv);
  producer.Run();
}