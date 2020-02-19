#include <rembrandt/network/ucx/context.h>
#include <rembrandt/broker/broker_node.h>
#include <rembrandt/broker/broker_node_config.h>

int main(int argc, char *argv[]) {
  UCP::Context context = UCP::Context(true);
  BrokerNodeConfig config = BrokerNodeConfig();
  BrokerNode broker_node = BrokerNode(context, config);
  broker_node.Run();
}
