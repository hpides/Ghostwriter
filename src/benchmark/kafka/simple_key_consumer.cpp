#include <boost/program_options.hpp>
#include <chrono>
#include <iostream>
#include <rembrandt/logging/throughput_logger.h>
#include <atomic>
#include <unordered_set>
#include <rdkafkacpp.h>
#include <hdr_histogram.h>
#include <rembrandt/logging/latency_logger.h>
#include <openssl/md5.h>

namespace po = boost::program_options;

void LogMD5(size_t batch_size, const char *buffer, size_t count);

int main(int argc, char *argv[]) {
  size_t max_batch_size;
  std::string log_directory;
  try {
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("max-batch-size",
         po::value(&max_batch_size)->default_value(1024 * 128),
         "Maximum size of an individual batch (sending unit) in bytes")
        ("log-dir",
         po::value(&log_directory)->default_value("/hpi/fs00/home/hendrik.makait/rembrandt/logs/20200727/processing_latency/kafka/"),
         "Directory to store throughput logs");

    po::variables_map variables_map;
    po::store(po::parse_command_line(argc, argv, desc), variables_map);
    po::notify(variables_map);

    if (variables_map.count("help")) {
      std::cout << "Usage: myExecutable [options]\n";
      std::cout << desc;
      exit(0);
    }
  } catch (const po::error &ex) {
    std::cout << ex.what() << std::endl;
    exit(1);
  }

  const size_t batch_count = 1000l * 1000 * 1000 * 80 / max_batch_size;
  std::atomic<long> counter = 0;

  std::string fileprefix = "kafka_consumer_" + std::to_string(max_batch_size);
//  WindowedLatencyLogger latency_logger = WindowedLatencyLogger(batch_count, 100);
  ThroughputLogger logger = ThroughputLogger(counter, log_directory, fileprefix + "_throughput", max_batch_size);

  std::string topic_name = "TestTopic";

  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
  std::string errstr;
  if (conf->set("bootstrap.servers", "10.150.1.12", errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (conf->set("enable.auto.commit", "false", errstr)
      != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (conf->set("enable.auto.offset.store", "false", errstr)
      != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }
//
//  if (conf->set("queued.min.messages", "1000000", errstr)
//      != RdKafka::Conf::CONF_OK) {
//    std::cerr << errstr << std::endl;
//    exit(1);
//  }

  if (conf->set("message.max.bytes", std::to_string(max_batch_size * 1.1), errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (conf->set("message.copy.max.bytes", "0", errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }


  if (conf->set("fetch.message.max.bytes", std::to_string(max_batch_size * 1.1), errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  if (conf->set("fetch.max.bytes", std::to_string(max_batch_size * 1.1), errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  }

  RdKafka::Consumer *consumer = RdKafka::Consumer::create(conf, errstr);
  if (!consumer) {
    std::cerr << "Failed to create consumer: " << errstr << std::endl;
    exit(1);
  }

  delete conf;

  std::atomic<bool> running = false;


  RdKafka::Topic *topic = RdKafka::Topic::create(consumer, topic_name,
                                                 tconf, errstr);
  if (!topic) {
    std::cerr << "Failed to create topic: " << errstr << std::endl;
    exit(1);
  }

  const size_t warmup_batch_count = batch_count / 10;
  consumer->start(topic, 0, RdKafka::Topic::OFFSET_BEGINNING);
  for (size_t count = 0; count < warmup_batch_count; count++) {
    if (count % (batch_count / 20) == 0) {
      printf("Warmup Iteration: %zu\n", count);
    }
    RdKafka::Message *msg = consumer->consume(topic, 0, 1000);
    delete msg;
  }

  counter = 0;
  logger.Start();
//  latency_logger.Activate();
  auto start = std::chrono::high_resolution_clock::now();

  for (size_t count = 0; count < batch_count; count++) {
    if (count % (batch_count / 20) == 0) {
      printf("Iteration: %zu\n", count);
    }
    RdKafka::Message *msg = consumer->consume(topic, 0, 1000);
    LogMD5(max_batch_size, (char *) msg->payload(), count);
    delete msg;
    ++counter;
  }
  auto stop = std::chrono::high_resolution_clock::now();
//  latency_logger.Output(log_directory, fileprefix);
  logger.Stop();
  running = false;
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Duration: " << duration.count() << " ms\n";
}

void LogMD5(size_t batch_size, const char *buffer, size_t count) {
  std::unique_ptr<unsigned char[]> md5 = std::make_unique<unsigned char[]>(MD5_DIGEST_LENGTH);
  MD5((const unsigned char *) buffer, batch_size, md5.get());
  std::clog << "MD5 #" << std::dec << count << ": ";
  for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
    std::clog << std::hex << ((int) md5[i]);
  }
  std::clog << "\n";
}