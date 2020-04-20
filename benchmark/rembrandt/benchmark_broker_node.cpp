#include <rembrandt/network/ucx/context.h>
#include <rembrandt/broker/broker_node.h>
#include <rembrandt/broker/broker_node_config.h>

int main(int argc, char *argv[]) {
  UCP::Context context = UCP::Context(true);
  UCP::Worker data_worker = UCP::Worker(context);
  UCP::Worker listening_worker = UCP::Worker(context);
  MessageGenerator message_generator = MessageGenerator();
  UCP::EndpointFactory endpoint_factory = UCP::EndpointFactory(message_generator);
  RequestProcessor request_processor(data_worker);
  ConnectionManager connection_manager(data_worker, &endpoint_factory);
  BrokerNodeConfig config = BrokerNodeConfig();
  config.storage_node_ip = (char *) "10.10.0.11";
  BrokerNode broker_node = BrokerNode(
      connection_manager,
      message_generator,
      request_processor,
      data_worker,
      listening_worker,
      config);
  broker_node.Run();
}
