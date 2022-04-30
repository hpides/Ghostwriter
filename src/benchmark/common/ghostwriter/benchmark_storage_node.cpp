#include <boost/program_options.hpp>
#include <rembrandt/network/ucx/context.h>
#include <rembrandt/storage/storage_node_config.h>
#include <rembrandt/storage/storage_node.h>
#include <iostream>
#include <rembrandt/storage/persistent_storage_region.h>
#include <rembrandt/storage/volatile_storage_region.h>

namespace po = boost::program_options;

StorageNodeConfig ParseOptions(int argc, char *const *argv) {
  std::string type;
  StorageNodeConfig config;
  try {
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("region-size,r",
         po::value(&config.region_size)->default_value(92l * 1024 * 1024 * 1024),
         "Size of the pre-allocated memory region in bytes")
        ("segment-size,s",
         po::value(&config.segment_size)->default_value(1024l * 1024 * 1024),
         "Size of an individual memory segment within the region")
        ("type", po::value(&type), "The type of memory used to store the data, 'persistent' or 'volatile'");

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
    if (type == "persistent") {
      config.type = StorageNodeConfig::Type::PERSISTENT;
    } else if (type == "volatile") {
      config.type = StorageNodeConfig::Type::VOLATILE;
    } else {
      std::cout << "Could not parse type: '" << type << "'" << std::endl;
      exit(1);
    }
  } catch (const po::error &ex) {
    std::cout << ex.what() << std::endl;
    exit(1);
  }
  return config;
}

int main(int argc, char *argv[]) {
  StorageNodeConfig config = ParseOptions(argc, argv);
  UCP::Context context = UCP::Context(true);
  StorageNode storage_node = StorageNode::Create(config, context);
  storage_node.Run();
}
