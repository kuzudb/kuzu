#pragma once

#include <cstdint>
#include <memory>
#include <mutex>

using namespace std;

namespace graphflow {
namespace storage {

struct BlockHandle {
public:
    explicit BlockHandle(uint64_t size, uint64_t numEntries)
        : freeSize(size), numEntries(numEntries) {
        buffer = make_unique<uint8_t[]>(size);
        blockPtr = buffer.get();
    }

public:
    uint64_t freeSize;
    uint8_t* blockPtr;
    uint64_t numEntries;

private:
    unique_ptr<uint8_t[]> buffer;
};

constexpr uint64_t DEFAULT_MEMORY_MANAGER_MAX_MEMORY = 1 << 30;

// Memory manager for allocating/reclaiming large intermediate memory blocks.
class MemoryManager {
public:
    MemoryManager(uint64_t maxMemory = DEFAULT_MEMORY_MANAGER_MAX_MEMORY)
        : maxMemory(maxMemory), usedMemory(0) {}

    int64_t getUsedMemory() const { return usedMemory; }

    int64_t getMaxMemory() const { return maxMemory; }

    unique_ptr<BlockHandle> allocateBlock(
        uint64_t size, bool initializeToZero = false, uint64_t numEntries = 0);

private:
    uint64_t maxMemory;
    uint64_t usedMemory;
    mutex memMgrLock;
};
} // namespace storage
} // namespace graphflow
