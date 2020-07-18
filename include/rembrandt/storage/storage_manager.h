#ifndef REMBRANDT_SRC_STORAGE_STORAGE_MANAGER_H_
#define REMBRANDT_SRC_STORAGE_STORAGE_MANAGER_H_

#include <unordered_map>
#include <queue>
#include <vector>
#include <memory>
#include <rembrandt/utils.h>
#include "rembrandt/storage/segment.h"
#include "storage_node_config.h"
#include "storage_region.h"

// TODO: Move identifies to utils/own file and remove inheritance.
struct PartitionIdentifier {
  PartitionIdentifier(uint32_t topic, uint32_t partition) : topic_id(topic), partition_id(partition) {}
  uint32_t topic_id;
  uint32_t partition_id;
  bool operator==(const PartitionIdentifier &other) const {
    return (topic_id == other.topic_id) && (partition_id == other.partition_id);
  }
};

struct PartitionIdentifierHash {
  size_t operator()(const PartitionIdentifier &partition_identifier) const {
    std::hash<uint32_t> hash_fn;
    return hash_fn(partition_identifier.topic_id)
        ^ hash_fn(partition_identifier.partition_id);
  }
};

struct SegmentIdentifier : public PartitionIdentifier {
  SegmentIdentifier(uint32_t topic, uint32_t partition, uint32_t segment) : PartitionIdentifier(topic, partition),
                                                                            segment_id(segment) {}
  uint32_t segment_id;
  bool operator==(const SegmentIdentifier &other) const {
    return (topic_id == other.topic_id) && (partition_id == other.partition_id) && (segment_id == other.segment_id);
  }
};

struct SegmentIdentifierHash {
  size_t operator()(const SegmentIdentifier &segment_identifier) const {
    std::hash<uint32_t> hash_fn;
    return hash_fn(segment_identifier.topic_id)
        ^ hash_fn(segment_identifier.partition_id)
        ^ hash_fn(segment_identifier.segment_id);
  }
};

class StorageManager {
 public:
  StorageManager() = delete;
  StorageManager(std::unique_ptr<StorageRegion> storage_region, StorageNodeConfig config);
  Segment *Allocate(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id);
  bool Free(uint32_t topic_id, uint32_t partition_id, uint32_t segment_id);
  bool HasFreeSegment() const;
  uint64_t GetSegmentSize() const;
  uint64_t GetOffset(void *location) const;
 private:
  std::unique_ptr<StorageRegion> storage_region_;
  std::vector<std::unique_ptr<Segment>> segments_;
  std::queue<Segment *> free_segments_;
  std::unordered_map<SegmentIdentifier, Segment *, SegmentIdentifierHash> allocated_segments_;
};

#endif //REMBRANDT_SRC_STORAGE_STORAGE_MANAGER_H_
