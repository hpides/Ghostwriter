#include <rembrandt/network/ucx/context.h>
#include <rembrandt/broker/broker_node.h>
#include <rembrandt/broker/broker_node_config.h>

int main(int argc, char *argv[]) {
  UCP::Context context = UCP::Context(true);
  UCP::Impl::Worker data_worker(context);
  UCP::Impl::Worker listening_worker(context);
  MessageGenerator message_generator;
  UCP::EndpointFactory endpoint_factory(message_generator);
  RequestProcessor request_processor(data_worker);
  ConnectionManager connection_manager(data_worker, &endpoint_factory);
  BrokerNodeConfig config;
  config.storage_node_ip = (char *) "10.10.0.11";
  BrokerNode broker_node(connection_manager,
                         message_generator,
                         request_processor,
                         data_worker,
                         listening_worker,
                         config);
  broker_node.Run();
}
