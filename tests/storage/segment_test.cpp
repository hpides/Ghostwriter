#include "gtest/gtest.h"

#include <rembrandt/storage/segment.h>

namespace {
class SegmentTest : public testing::Test {
 protected:
  void SetUp() override {
    size_ = 128;
    location_ = aligned_alloc(alignof(SegmentHeader), size_);
    segment_ = std::move(Segment(location_, size_));
  };

  void TearDown() override {};

  Segment segment_;
  uint64_t size_;
  void *location_;
};
}

TEST_F(SegmentTest, DefaultConstructor) {
  Segment segment;
  EXPECT_EQ(sizeof(SegmentHeader), segment.GetSize());
  EXPECT_TRUE(segment.IsFree());
  EXPECT_EQ(-1, segment.GetTopicId());
  EXPECT_EQ(-1, segment.GetPartitionId());
  EXPECT_EQ(-1, segment.GetSegmentId());
  EXPECT_EQ(segment.GetDataOffset(), segment.GetLastCommittedOffset());
}

TEST_F(SegmentTest, Constructor) {
  EXPECT_EQ(128, segment_.GetSize());
  EXPECT_EQ(location_, segment_.GetMemoryLocation());
  EXPECT_TRUE(segment_.IsFree());
  EXPECT_EQ(-1, segment_.GetTopicId());
  EXPECT_EQ(-1, segment_.GetPartitionId());
  EXPECT_EQ(-1, segment_.GetSegmentId());
  EXPECT_EQ(segment_.GetDataOffset(), segment_.GetLastCommittedOffset());
}

TEST_F(SegmentTest, MoveConstructor) {
  segment_.Allocate(1, 2, 3);
  segment_.SetLastCommittedOffset(42);
  Segment destination(std::move(segment_));

  EXPECT_EQ(128, destination.GetSize());
  EXPECT_EQ(location_, destination.GetMemoryLocation());
  EXPECT_FALSE(destination.IsFree());
  EXPECT_EQ(1, destination.GetTopicId());
  EXPECT_EQ(2, destination.GetPartitionId());
  EXPECT_EQ(3, destination.GetSegmentId());
  EXPECT_EQ(42, destination.GetLastCommittedOffset());

  EXPECT_EQ(sizeof(SegmentHeader), segment_.GetSize());
  EXPECT_TRUE(segment_.IsFree());
  EXPECT_EQ(-1, segment_.GetTopicId());
  EXPECT_EQ(-1, segment_.GetPartitionId());
  EXPECT_EQ(-1, segment_.GetSegmentId());
  EXPECT_EQ(segment_.GetDataOffset(), segment_.GetLastCommittedOffset());
}

TEST_F(SegmentTest, MoveAssignment) {
  segment_.Allocate(1, 2, 3);
  segment_.SetLastCommittedOffset(42);
  Segment destination = std::move(segment_);

  EXPECT_EQ(128, destination.GetSize());
  EXPECT_EQ(location_, destination.GetMemoryLocation());
  EXPECT_FALSE(destination.IsFree());
  EXPECT_EQ(1, destination.GetTopicId());
  EXPECT_EQ(2, destination.GetPartitionId());
  EXPECT_EQ(3, destination.GetSegmentId());
  EXPECT_EQ(42, destination.GetLastCommittedOffset());

  EXPECT_EQ(sizeof(SegmentHeader), segment_.GetSize());
  EXPECT_TRUE(segment_.IsFree());
  EXPECT_EQ(-1, segment_.GetTopicId());
  EXPECT_EQ(-1, segment_.GetPartitionId());
  EXPECT_EQ(-1, segment_.GetSegmentId());
  EXPECT_EQ(segment_.GetDataOffset(), segment_.GetLastCommittedOffset());
}

TEST_F(SegmentTest, Allocate) {
  Segment segment;
  EXPECT_TRUE(segment_.Allocate(1, 2, 3));
  EXPECT_EQ(1, segment_.GetTopicId());
  EXPECT_EQ(2, segment_.GetPartitionId());
  EXPECT_EQ(3, segment_.GetSegmentId());
  EXPECT_FALSE(segment_.IsFree());
  EXPECT_EQ(segment_.GetDataOffset(), segment_.GetLastCommittedOffset());
}

TEST_F(SegmentTest, AllocateNegativeIds) {
  EXPECT_FALSE(segment_.Allocate(-2, 2, 3));
  EXPECT_FALSE(segment_.Allocate(1, -2, 3));
  EXPECT_FALSE(segment_.Allocate(1, 2, -3));
  EXPECT_TRUE(segment_.IsFree());
  EXPECT_EQ(-1, segment_.GetTopicId());
  EXPECT_EQ(-1, segment_.GetPartitionId());
  EXPECT_EQ(-1, segment_.GetSegmentId());
  EXPECT_EQ(segment_.GetDataOffset(), segment_.GetLastCommittedOffset());
}

TEST_F(SegmentTest, AllocateNotFree) {
  segment_.Allocate(1, 2, 3);
  EXPECT_FALSE(segment_.Allocate(2, 3, 4));
  EXPECT_EQ(1, segment_.GetTopicId());
  EXPECT_EQ(2, segment_.GetPartitionId());
  EXPECT_EQ(3, segment_.GetSegmentId());
  EXPECT_FALSE(segment_.IsFree());
  EXPECT_EQ(segment_.GetDataOffset(), segment_.GetLastCommittedOffset());
}

TEST_F(SegmentTest, Free) {
  segment_.Allocate(1, 2, 3);
  segment_.SetLastCommittedOffset(42);
  segment_.Free();
  EXPECT_TRUE(segment_.IsFree());
  EXPECT_EQ(-1, segment_.GetTopicId());
  EXPECT_EQ(-1, segment_.GetPartitionId());
  EXPECT_EQ(-1, segment_.GetSegmentId());
  EXPECT_EQ(segment_.GetDataOffset(), segment_.GetLastCommittedOffset());
}

TEST_F(SegmentTest, GetDataOffset) {
  EXPECT_EQ(32, Segment::GetDataOffset());
  EXPECT_EQ(sizeof(SegmentHeader), Segment::GetDataOffset());
}

TEST_F(SegmentTest, GetLastCommittedOffset) {
  EXPECT_EQ(segment_.GetLastCommittedOffset(), segment_.GetDataOffset());
  segment_.SetLastCommittedOffset(42);
  EXPECT_EQ(42, segment_.GetLastCommittedOffset());
}

TEST_F(SegmentTest, GetOffsetOfLastCommittedOffset) {
  segment_.SetLastCommittedOffset(42);
  auto last_committed_offset_pointer =
      (uint64_t *) ((char *) segment_.GetMemoryLocation() + segment_.GetOffsetOfLastCommittedOffset());
  EXPECT_EQ(42, *last_committed_offset_pointer);
  *last_committed_offset_pointer = 43;
  EXPECT_EQ(43, segment_.GetLastCommittedOffset());
}