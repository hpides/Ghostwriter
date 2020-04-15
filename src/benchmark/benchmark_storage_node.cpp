#include <rembrandt/network/ucx/context.h>
#include <rembrandt/storage/storage_node_config.h>
#include <rembrandt/storage/storage_node.h>

int main(int argc, char *argv[]) {
  UCP::Context context = UCP::Context(true);
  UCP::Worker data_worker = UCP::Worker(context);
  UCP::Worker listening_worker = UCP::Worker(context);
  StorageNodeConfig config = StorageNodeConfig();
  config.region_size = 1000l * 1000 * 1000 * 1; // 10 GB
  config.segment_size = 1000l * 1000 * 1000 * 1; // 10 GB
  UCP::MemoryRegion memory_region(context, config.region_size);
  MessageGenerator message_generator = MessageGenerator();
  RKeyServer r_key_server(memory_region);
  StorageNode storage_node = StorageNode(
      data_worker,
      listening_worker,
      memory_region,
      r_key_server,
      message_generator,
      config);
  storage_node.Run();
}
