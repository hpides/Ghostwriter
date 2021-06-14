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

TEST_F(SegmentTest, Constructor) {
  EXPECT_EQ(location_, segment_.GetMemoryLocation());
  EXPECT_TRUE(segment_.IsFree());
  EXPECT_EQ(-1, segment_.GetTopicId());
  EXPECT_EQ(-1, segment_.GetPartitionId());
  EXPECT_EQ(-1, segment_.GetSegmentId());
  EXPECT_EQ(segment_.GetDataOffset(), segment_.GetCommitOffset());
}

TEST_F(SegmentTest, MoveConstructor) {
  segment_.Allocate(1, 2, 3, 0);
  segment_.SetCommitOffset(42);
  Segment destination(std::move(segment_));

  EXPECT_EQ(location_, destination.GetMemoryLocation());
  EXPECT_FALSE(destination.IsFree());
  EXPECT_EQ(1, destination.GetTopicId());
  EXPECT_EQ(2, destination.GetPartitionId());
  EXPECT_EQ(3, destination.GetSegmentId());
  EXPECT_EQ(42, destination.GetCommitOffset());

  EXPECT_TRUE(segment_.IsFree());
  EXPECT_EQ(-1, segment_.GetTopicId());
  EXPECT_EQ(-1, segment_.GetPartitionId());
  EXPECT_EQ(-1, segment_.GetSegmentId());
  EXPECT_EQ(segment_.GetDataOffset(), segment_.GetCommitOffset());
}

TEST_F(SegmentTest, MoveAssignment) {
  segment_.Allocate(1, 2, 3, 0);
  segment_.SetCommitOffset(42);
  Segment destination = std::move(segment_);

  EXPECT_EQ(location_, destination.GetMemoryLocation());
  EXPECT_FALSE(destination.IsFree());
  EXPECT_EQ(1, destination.GetTopicId());
  EXPECT_EQ(2, destination.GetPartitionId());
  EXPECT_EQ(3, destination.GetSegmentId());
  EXPECT_EQ(42, destination.GetCommitOffset());

  EXPECT_TRUE(segment_.IsFree());
  EXPECT_EQ(-1, segment_.GetTopicId());
  EXPECT_EQ(-1, segment_.GetPartitionId());
  EXPECT_EQ(-1, segment_.GetSegmentId());
  EXPECT_EQ(segment_.GetDataOffset(), segment_.GetCommitOffset());
}

TEST_F(SegmentTest, Allocate) {
  EXPECT_TRUE(segment_.Allocate(1, 2, 3, 0));
  EXPECT_EQ(1, segment_.GetTopicId());
  EXPECT_EQ(2, segment_.GetPartitionId());
  EXPECT_EQ(3, segment_.GetSegmentId());
  EXPECT_FALSE(segment_.IsFree());
  EXPECT_EQ(segment_.GetDataOffset(), segment_.GetCommitOffset());
}

TEST_F(SegmentTest, AllocateNegativeIds) {
  EXPECT_FALSE(segment_.Allocate(-2, 2, 3, 0));
  EXPECT_FALSE(segment_.Allocate(1, -2, 3, 0));
  EXPECT_FALSE(segment_.Allocate(1, 2, -3, 0));
  EXPECT_TRUE(segment_.IsFree());
  EXPECT_EQ(-1, segment_.GetTopicId());
  EXPECT_EQ(-1, segment_.GetPartitionId());
  EXPECT_EQ(-1, segment_.GetSegmentId());
  EXPECT_EQ(segment_.GetDataOffset(), segment_.GetCommitOffset());
}

TEST_F(SegmentTest, AllocateNotFree) {
  segment_.Allocate(1, 2, 3, 0);
  EXPECT_FALSE(segment_.Allocate(2, 3, 4, 0));
  EXPECT_EQ(1, segment_.GetTopicId());
  EXPECT_EQ(2, segment_.GetPartitionId());
  EXPECT_EQ(3, segment_.GetSegmentId());
  EXPECT_FALSE(segment_.IsFree());
  EXPECT_EQ(segment_.GetDataOffset(), segment_.GetCommitOffset());
}

TEST_F(SegmentTest, Free) {
  segment_.Allocate(1, 2, 3, 0);
  segment_.SetCommitOffset(42);
  segment_.Free();
  EXPECT_TRUE(segment_.IsFree());
  EXPECT_EQ(-1, segment_.GetTopicId());
  EXPECT_EQ(-1, segment_.GetPartitionId());
  EXPECT_EQ(-1, segment_.GetSegmentId());
  EXPECT_EQ(segment_.GetDataOffset(), segment_.GetCommitOffset());
}

TEST_F(SegmentTest, GetDataOffset) {
  EXPECT_EQ(40, Segment::GetDataOffset());
  EXPECT_EQ(sizeof(SegmentHeader), Segment::GetDataOffset());
}

TEST_F(SegmentTest, GetCommitOffset) {
  EXPECT_EQ(segment_.GetCommitOffset(), segment_.GetDataOffset());
  segment_.SetCommitOffset(42);
  EXPECT_EQ(42, segment_.GetCommitOffset());
}

TEST_F(SegmentTest, GetOffsetOfCommitOffset) {
  segment_.SetCommitOffset(42);
  auto last_committed_offset_pointer =
      (uint64_t *) ((char *) segment_.GetMemoryLocation() + segment_.GetOffsetOfCommitOffset());
  EXPECT_EQ(42, *last_committed_offset_pointer);
  *last_committed_offset_pointer = 43;
  EXPECT_EQ(43, segment_.GetCommitOffset());
}