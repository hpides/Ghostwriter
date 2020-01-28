@0xe95493abab1576ee;

# Segment allocation and deallocation

struct Allocate {
  topicId @0 :UInt32;
  partitionId @1 :UInt32;
  segmentId @2 :UInt32;
}

struct Allocated {
  topicId @0 :UInt32;
  partitionId @1 :UInt32;
  segmentId @2 :UInt32;
  offset @3 :UInt64;
  size @4 :UInt64;
}

struct AllocateFailed {
  topicId @0 :UInt32;
  partitionId @1 :UInt32;
  segmentId @2 :UInt32;
  errorCode @3 :UInt16;
  errorMessage @4 :Text;
}

struct Free {
  topicId @0 :UInt32;
  partitionId @1 :UInt32;
  segmentId @2 :UInt32;
}

struct Freed {
  topicId @0 :UInt32;
  partitionId @1 :UInt32;
  segmentId @2 :UInt32;
}

struct FreeFailed {
  topicId @0 :UInt32;
  partitionId @1 :UInt32;
  segmentId @2 :UInt32;
  errorCode @3 :UInt16;
  errorMessage @4 :Text;
}


struct SendOutline {
    topicId @0 :UInt32;
    partitionId @1 :UInt32;
    numMessages @2 :UInt32;
    totalSize @3 :UInt32;
}

struct


struct Message {
  message: union {
  # Segment allocation and deallocation
    allocate @0 :Allocate;
    allocated @1 :Allocated;
    allocateFailed @2 :AllocateFailed;
    free @3 :Free;
    freed @4 :Freed;
    freeFailed @5 :FreeFailed;
  }
}