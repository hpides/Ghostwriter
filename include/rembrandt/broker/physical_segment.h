#ifndef REMBRANDT_SRC_BROKER_PHYSICAL_SEGMENT_H_
#define REMBRANDT_SRC_BROKER_PHYSICAL_SEGMENT_H_

#include <cstdint>
class PhysicalSegment {
 public:
  PhysicalSegment(uint64_t location);
  uint64_t GetLocation() const;
  uint64_t GetLocationOfCommitOffset() const;
  uint64_t GetLocationOfWriteOffset() const;
  uint64_t GetLocationOfData() const;
 private:
  const uint64_t location_;

};

#endif //REMBRANDT_SRC_BROKER_PHYSICAL_SEGMENT_H_
