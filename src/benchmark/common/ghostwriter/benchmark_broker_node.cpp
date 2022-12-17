#include <boost/program_options.hpp>
#include <rembrandt/network/ucx/context.h>
#include <rembrandt/broker/broker_node.h>
#include <rembrandt/broker/broker_node_config.h>
#include <iostream>
#include <rembrandt/broker/partition.h>


BrokerNodeConfig ParseOptions(int argc, char *const *argv) {
  namespace po = boost::program_options;
  BrokerNodeConfig config;
  std::string mode_str;
  try {
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("storage-node-ip",
         po::value(&config.storage_node_ip)->default_value("10.150.1.12"),
         "IP address of the storage node")
        ("storage-node-port",
         po::value(&config.storage_node_port)->default_value(13350),
         "Port number of the storage node")
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
      config.mode = Partition::Mode::EXCLUSIVE;
    } else if (mode_str == "concurrent") {
      config.mode = Partition::Mode::CONCURRENT;
    } else {
      std::cout << "Could not parse mode: '" << mode_str << "'" << std::endl;
      exit(1);
    }
  } catch (const po::error &ex) {
    std::cout << ex.what() << std::endl;
    exit(1);
  }
  return config;
}

int main(int argc, char *argv[]) {
  BrokerNodeConfig config = ParseOptions(argc, argv);

  UCP::Context context = UCP::Context(true);
  std::unique_ptr<BrokerNode> broker_p = BrokerNode::Create(config, context);
  broker_p->AssignPartition(1, 1, config.mode);
  broker_p->Run();
}
