#include <boost/program_options.hpp>
#include <rembrandt/network/ucx/context.h>
#include <rembrandt/broker/broker_node.h>
#include <rembrandt/broker/broker_node_config.h>
#include <iostream>
#include <rembrandt/broker/partition.h>

namespace po = boost::program_options;

int main(int argc, char *argv[]) {
  BrokerNodeConfig config;
  try {
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("storage-node-ip",
         po::value(&config.storage_node_ip)->default_value("10.150.1.12"),
         "IP address of the storage node")
        ("storage-node-port",
         po::value(&config.storage_node_port)->default_value(13350),
         "Port number of the storage node");
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

  UCP::Context context = UCP::Context(true);
  std::unique_ptr<UCP::Impl::Worker> client_worker = std::make_unique<UCP::Impl::Worker>(context);
  std::unique_ptr<UCP::Impl::Worker> data_worker = std::make_unique<UCP::Impl::Worker>(context);
  std::unique_ptr<UCP::Impl::Worker> listening_worker = std::make_unique<UCP::Impl::Worker>(context);
  std::unique_ptr<Server>
      server = std::make_unique<Server>(std::move(data_worker), std::move(listening_worker), config.server_port);
  std::unique_ptr<MessageGenerator> message_generator = std::make_unique<MessageGenerator>();
  UCP::EndpointFactory endpoint_factory;
  RequestProcessor request_processor(*client_worker);
  ConnectionManager connection_manager(*client_worker, &endpoint_factory, *message_generator, request_processor);

  BrokerNode broker_node(std::move(server), connection_manager,
                         std::move(message_generator),
                         request_processor,
                         std::move(client_worker),
                         config);
  broker_node.AssignPartition(1, 1, Partition::Mode::EXCLUSIVE);
  broker_node.Run();
}
