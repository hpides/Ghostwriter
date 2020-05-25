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
         po::value(&config.region_size)->default_value(11l * 1024 * 1024 * 1024),
         "Size of the pre-allocated memory region in bytes")
        ("segment-size,s",
         po::value(&config.segment_size)->default_value(1l * 1024 * 1024 * 1024),
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
  std::unique_ptr<UCP::Impl::Worker> data_worker  = std::make_unique<UCP::Impl::Worker>(context);
  std::unique_ptr<UCP::Impl::Worker> listening_worker  = std::make_unique<UCP::Impl::Worker>(context);
  std::unique_ptr<Server> server = std::make_unique<Server>(std::move(data_worker), std::move(listening_worker), config.server_port);
  std::unique_ptr<StorageRegion> storage_region = std::make_unique<StorageRegion>(config.region_size, alignof(SegmentHeader));
  std::unique_ptr<UCP::MemoryRegion> memory_region = std::make_unique<UCP::MemoryRegion>(context, *storage_region);
  std::unique_ptr<StorageManager> storage_manager = std::make_unique<StorageManager>(std::move(storage_region), config);
  std::unique_ptr<MessageGenerator> message_generator = std::make_unique<MessageGenerator>();
  StorageNode storage_node(std::move(server),
                           std::move(memory_region),
                           std::move(message_generator),
                           std::move(storage_manager),
                           config);
  storage_node.Run();
}

