#include <rembrandt/network/ucx/context.h>
#include <rembrandt/broker/broker_node.h>
#include <rembrandt/broker/broker_node_config.h>

int main(int argc, char *argv[]) {
  UCP::Context context = UCP::Context(false);
  BrokerNodeConfig config = BrokerNodeConfig();
  BrokerNode broker_node = BrokerNode(context, config, <#initializer#>);
  broker_node.Run();
}
