#include <rembrandt/network/ucx/context.h>
#include <rembrandt/storage/storage_node_config.h>
#include <rembrandt/storage/storage_node.h>

int main(int argc, char *argv[]) {
  UCP::Context context = UCP::Context(true);
  StorageNodeConfig config = StorageNodeConfig();
  config.region_size = 1000l * 1000 * 1000 * 10; // 10 GB
  config.segment_size = 1000l * 1000 * 1000 * 10; // 10 GB
  UCP::MemoryRegion memory_region(context);
  MessageGenerator message_generator = MessageGenerator();
  RKeyServer r_key_server(memory_region);
  StorageNode storage_node = StorageNode(context, memory_region, r_key_server, message_generator, config);
  storage_node.Run();
}
