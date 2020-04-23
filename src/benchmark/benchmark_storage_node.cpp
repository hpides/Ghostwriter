#include <rembrandt/network/ucx/context.h>
#include <rembrandt/storage/storage_node_config.h>
#include <rembrandt/storage/storage_node.h>

int main(int argc, char *argv[]) {
  UCP::Context context = UCP::Context(true);
  UCP::Impl::Worker data_worker(context);
  UCP::Impl::Worker listening_worker(context);
  StorageNodeConfig config;
  config.region_size = 1000l * 1000 * 1000 * 10; // 10 GB
  config.segment_size = 1000l * 1000 * 1000 * 10; // 10 GB
  UCP::MemoryRegion memory_region(context, config.region_size);
  MessageGenerator message_generator;
  RKeyServer r_key_server(memory_region);
  StorageNode storage_node(data_worker,
                           listening_worker,
                           memory_region,
                           r_key_server,
                           message_generator,
                           config);
  storage_node.Run();
}
