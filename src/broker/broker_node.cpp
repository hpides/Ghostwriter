#include "rembrandt/broker/broker_node.h"

static

int main (int argc, char *argv[]) {
  UCP::Context context = UCP::Context(false);
  Server server_ = Server(context);
  server.Listen();
}