#include "src/storage/include/memory_manager.h"

#include <cstring>

namespace graphflow {
namespace storage {

unique_ptr<BlockHandle> MemoryManager::allocateBlock(
    uint64_t size, bool initializeToZero, uint64_t numEntries) {
    lock_guard<mutex> lock(memMgrLock);

    if (usedMemory + size >= maxMemory) {
        throw invalid_argument("Out of memory");
    }

    auto blockHandle = make_unique<BlockHandle>(size, numEntries);
    if (initializeToZero) {
        memset(blockHandle->blockPtr, 0, size);
    }
    usedMemory += size;

    return blockHandle;
}
} // namespace storage
} // namespace graphflow
