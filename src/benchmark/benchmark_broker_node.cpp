#include <rembrandt/network/ucx/context.h>
#include <rembrandt/broker/broker_node.h>
#include <rembrandt/broker/broker_node_config.h>

int main(int argc, char *argv[]) {
  UCP::Context context = UCP::Context(true);
  UCP::Worker worker = UCP::Worker(context);
  UCP::EndpointFactory endpoint_factory;
  RequestProcessor request_processor(worker);
  ConnectionManager connection_manager(worker, &endpoint_factory);
  MessageGenerator message_generator = MessageGenerator();
  BrokerNodeConfig config = BrokerNodeConfig();
  config.storage_node_ip = (char *) "192.168.5.40";
  BrokerNode broker_node = BrokerNode(context,
                                      connection_manager,
                                      message_generator,
                                      request_processor,
                                      worker,
                                      config);
  broker_node.Run();
}
