#include "../include/rembrandt/storage/segment.h"
#include "gtest/gtest.h"

TEST(Segment, Construction) {
  void *location = malloc(1000);
  Segment segment = Segment(location, 1000);
  ASSERT_TRUE(segment.IsFree());
  ASSERT_EQ(segment.GetMemoryLocation(), location);
  ASSERT_EQ(segment.GetTopicId(), 0); // default to 0 for free segments
  ASSERT_EQ(segment.GetPartitionId(), 0); // default to 0 for free segments
  ASSERT_EQ(segment.GetSegmentId(), 0); // default to 0 for free segments
}

TEST(Segment, Allocation) {
  void *location = malloc(1000);
  Segment segment = Segment(location, 1000);
  segment.Allocate(1,2,3);
  ASSERT_FALSE(segment.IsFree());
  ASSERT_EQ(segment.GetTopicId(), 1);
  ASSERT_EQ(segment.GetPartitionId(), 2);

}

TEST(Segment, Recovery) {
  void *location = malloc(1000);
  Segment segment = Segment(location, 1000);
  ASSERT_TRUE(segment.IsFree());

  segment.Allocate(1, 2, 3);
  ASSERT_FALSE(segment.IsFree());

  Segment recovered = Segment::FromLocation(segment.GetMemoryLocation());
  ASSERT_FALSE(recovered.IsFree());
  free(location);
}