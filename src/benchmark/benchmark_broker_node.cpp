#include <rembrandt/network/ucx/context.h>
#include <rembrandt/broker/broker_node.h>
#include <rembrandt/broker/broker_node_config.h>

int main(int argc, char *argv[]) {
  UCP::Context context = UCP::Context(false);
  BrokerNodeConfig config = BrokerNodeConfig();
  MessageGenerator message_generator = MessageGenerator();
  BrokerNode broker_node = BrokerNode(context, message_generator, config);
  broker_node.Run();
}
