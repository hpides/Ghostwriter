#include <rembrandt/protocol/protocol.h>

size_t Protocol::GetEffectiveBatchSize(size_t batch_size, Partition::Mode mode) {
        switch (mode) {
            case Partition::Mode::EXCLUSIVE: return batch_size;
            case Partition::Mode::CONCURRENT: return Protocol::GetConcurrentBatchSize(batch_size);
            default: throw std::runtime_error("Partition mode not available!");

        }
}

uint64_t Protocol::GetConcurrentBatchSize(uint64_t batch_size) {
    return batch_size + sizeof(TIMEOUT_FLAG);
}