#pragma once

#include <cstdint>
#include <memory>
#include <mutex>

using namespace std;

namespace graphflow {
namespace common {

struct MemoryBlock {
public:
    explicit MemoryBlock(uint64_t size) : size(size) {
        buffer = make_unique<uint8_t[]>(size);
        data = buffer.get();
    }

public:
    uint64_t size;
    uint8_t* data;

private:
    unique_ptr<uint8_t[]> buffer;
};

// Memory manager for allocating/reclaiming large intermediate memory blocks.
class MemoryManager {
    static constexpr uint64_t DEFAULT_MEMORY_MANAGER_MAX_MEMORY = 1 << 30;

public:
    explicit MemoryManager(uint64_t maxMemory = DEFAULT_MEMORY_MANAGER_MAX_MEMORY)
        : maxMemory(maxMemory), usedMemory(0) {}

    unique_ptr<MemoryBlock> allocateBlock(uint64_t size, bool initializeToZero = false);

public:
    uint64_t maxMemory;
    uint64_t usedMemory;

private:
    mutex memMgrLock;
};
} // namespace common
} // namespace graphflow
