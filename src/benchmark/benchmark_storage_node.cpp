#include <boost/program_options.hpp>
#include <rembrandt/network/ucx/context.h>
#include <rembrandt/storage/storage_node_config.h>
#include <rembrandt/storage/storage_node.h>
#include <iostream>

namespace po = boost::program_options;

int main(int argc, char *argv[]) {
  StorageNodeConfig config;
  try {
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("region-size,r",
         po::value(&config.region_size)->default_value(10l * 1000 * 1000 * 1000),
         "Size of the pre-allocated memory region in bytes")
        ("segment-size,s",
         po::value(&config.segment_size)->default_value(10l * 1000 * 1000 * 1000),
         "Size of an individual memory segment within the region");

    po::variables_map variables_map;
    po::store(po::parse_command_line(argc, argv, desc), variables_map);
    po::notify(variables_map);

    if (variables_map.count("help")) {
      std::cout << "Usage: myExecutable [options]\n";
      std::cout << desc;
      exit(0);
    }

    if (variables_map["region-size"].as<uint64_t>() < variables_map["segment-size"].as<uint64_t>()) {
      throw po::error("Option 'region-size' must be at least as large as option 'segment-size', is "
                          + std::to_string(variables_map["region-size"].as<uint64_t>()) + " < "
                          + std::to_string(variables_map["segment-size"].as<uint64_t>()));
    }
  } catch (const po::error &ex) {
    std::cout << ex.what() << std::endl;
    exit(1);
  }

  UCP::Context context = UCP::Context(true);
  UCP::Impl::Worker data_worker(context);
  UCP::Impl::Worker listening_worker(context);
  UCP::MemoryRegion memory_region(context, config.region_size);
  MessageGenerator message_generator;
  RKeyServer r_key_server(memory_region);
  StorageNode storage_node(data_worker,
                           listening_worker,
                           memory_region,
                           r_key_server,
                           message_generator,
                           config);
  storage_node.Run();
}

