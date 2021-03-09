#pragma once

#include <cstdint>
#include <memory>
#include <mutex>

#include "src/common/include/types.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

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

// Memory manager for allocating/reclaiming large intermediate memory blocks.
class MemoryManager {
public:
    MemoryManager(int64_t maxMemory) : maxMemory(maxMemory), usedMemory(0) {}

    int64_t getUsedMemory() { return usedMemory; }

    int64_t getMaxMemory() { return maxMemory; }

    unique_ptr<BlockHandle> allocateBlock(uint64_t size, int numEntries = 0);

private:
    mutex memMgrLock;
    uint64_t maxMemory;
    uint64_t usedMemory;
};
} // namespace processor
} // namespace graphflow
