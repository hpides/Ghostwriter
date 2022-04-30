#include <boost/program_options.hpp>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_hash_map.h>
#include <cstring>

#include "YahooBenchmark/GhostwriterYSB.cpp"
namespace po = boost::program_options;

void ReadIntoMemory(std::string input_file, char **buffer_p, long *fsize_p) {
  FILE *f = fopen(input_file.c_str(), "rb");
  fseek(f, 0, SEEK_END);
  long fsize = ftell(f);
  *fsize_p = fsize;
  fseek(f, 0, SEEK_SET);  /* same as rewind(f); */

  char *buffer = (char *) malloc(fsize + 1);
  fread(buffer, 1, fsize, f);
  fclose(f);

  buffer[fsize] = 0;
  *buffer_p = buffer;
}

int main(int argc, char *argv[]) {
  std::string input_file;
  size_t max_batch_size;
  try {
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("max-batch-size",
         po::value(&max_batch_size)->default_value(524288),
         "Maximum size of an individual batch (sending unit) in bytes")
        ("input-dir",
         po::value(&input_file)->default_value(
             "/hpi/fs00/home/hendrik.makait/ghostwriter-experiments/data/10m/ysb0.bin"),
         "File to load generated YSB data");

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

  const size_t kNumBuffers = 24;
  tbb::concurrent_bounded_queue<char *> free_buffers;
  tbb::concurrent_bounded_queue<char *> received_buffers;
  tbb::concurrent_hash_map<uint64_t, uint64_t> counts;
  for (size_t i = 0; i < kNumBuffers; i++) {
    free_buffers.push((char *) &i);
  }

  long fsize;
  char *buffer;
  ReadIntoMemory(input_file, &buffer, &fsize);

  printf("Init YSB...");
  GhostwriterYSB ysb(max_batch_size, free_buffers, received_buffers);
  printf("Start benchmark...");
  SystemConf::getInstance().BUNDLE_SIZE = max_batch_size;
  SystemConf::getInstance().BATCH_SIZE = max_batch_size;
  SystemConf::getInstance().CIRCULAR_BUFFER_SIZE = 8388608;
  //  DataProcessor data_processor(config.max_batch_size, free_buffers, received_buffers, counts);
  std::thread data_processor_thread(&GhostwriterYSB::runBenchmark, ysb, true);

  printf("Benchmark running...");
  char *dummy;
  size_t batch_size = (max_batch_size / 128) * 128;
  const size_t batch_count = fsize / batch_size;
  for (size_t loop = 0; loop < 100; loop++) {
    for (size_t count = 0; count < batch_count; count++) {
//    if (count % (batch_count / 2) == 0) {
//      printf("Iteration: %zu\n", count);
//    }

      free_buffers.pop(dummy);
//    free_buffers.push(dummy);
      received_buffers.push(buffer);
    }
  }
  data_processor_thread.join();
}
