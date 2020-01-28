#include "rembrandt/storage/segment.h"
#include "rembrandt/storage/storage_node.h"

#include <sys/mman.h>

StorageNode::StorageNode(uint64_t region_size, uint64_t segment_size) {
  void * memory_region = malloc(region_size);

  for (int i = 0; i < region_size / segment_size; i++) {
    Segment(segment_size);
  }
}