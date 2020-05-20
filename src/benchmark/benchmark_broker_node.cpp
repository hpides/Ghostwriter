#include <boost/program_options.hpp>
#include <rembrandt/network/ucx/context.h>
#include <rembrandt/broker/broker_node.h>
#include <rembrandt/broker/broker_node_config.h>
#include <iostream>

namespace po = boost::program_options;

int main(int argc, char *argv[]) {
  BrokerNodeConfig config;
  try {
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("storage-node-ip",
         po::value(&config.storage_node_ip)->default_value("10.10.0.11"),
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
  UCP::Impl::Worker data_worker(context);
  UCP::Impl::Worker listening_worker(context);
  MessageGenerator message_generator;
  UCP::EndpointFactory endpoint_factory;
  RequestProcessor request_processor(data_worker);
  ConnectionManager connection_manager(data_worker, &endpoint_factory, message_generator, request_processor);

  BrokerNode broker_node(connection_manager,
                         message_generator,
                         request_processor,
                         data_worker,
                         listening_worker,
                         config);
  broker_node.Run();
}
