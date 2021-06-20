#include "src/common/include/memory_manager.h"

#include <cstring>

namespace graphflow {
namespace common {

unique_ptr<MemoryBlock> MemoryManager::allocateBlock(uint64_t size, bool initializeToZero) {
    lock_guard<mutex> lock(memMgrLock);

    if (usedMemory + size >= maxMemory) {
        throw invalid_argument("Out of memory");
    }

    auto blockHandle = make_unique<MemoryBlock>(size);
    if (initializeToZero) {
        memset(blockHandle->data, 0, size);
    }
    usedMemory += size;

    return blockHandle;
}
} // namespace common
} // namespace graphflow
