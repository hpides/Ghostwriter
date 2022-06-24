#ifndef REMBRANDT_SRC_PROTOCOL_PROTOCOL_H_
#define REMBRANDT_SRC_PROTOCOL_PROTOCOL_H_

#include <cstdlib>
#include <rembrandt/broker/partition.h>

namespace Protocol {
    static constexpr uint64_t STAGED_FLAG = 0ul;
    static constexpr uint64_t COMMIT_FLAG = 1ul;
    static constexpr uint64_t TIMEOUT_FLAG = 2ul;
    static constexpr uint64_t COMMIT_FILL[4] = {COMMIT_FLAG, COMMIT_FLAG, COMMIT_FLAG, COMMIT_FLAG};
    uint64_t GetConcurrentBatchSize(uint64_t batch_size);
    size_t GetEffectiveBatchSize(size_t batch_size, Partition::Mode mode);
} 

#endif // REMBRANDT_SRC_PROTOCOL_PROTOCOL_H_