#include "src/processor/include/memory_manager.h"

namespace graphflow {
namespace processor {

unique_ptr<BlockHandle> MemoryManager::allocateBlock(uint64_t size, int numEntries) {
    lock_guard<mutex> lock(memMgrLock);
    if (usedMemory + size >= maxMemory) {
        throw invalid_argument("Out of memory");
    }
    auto blockHandle = make_unique<BlockHandle>(size, numEntries);
    std::fill_n(blockHandle->blockPtr, size, 0);
    usedMemory += size;
    return blockHandle;
}

} // namespace processor
} // namespace graphflow
