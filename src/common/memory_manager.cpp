#include "src/common/include/memory_manager.h"

#include <cstring>

namespace graphflow {
namespace common {

unique_ptr<BMBackedMemoryBlock> MemoryManager::allocateBMBackedBlock(bool initializeToZero) {
    lock_guard<mutex> lock(memMgrLock);
    uint32_t pageIdx;
    uint8_t* data;
    if (freePages.empty()) {
        pageIdx = fh->addNewPage();
    } else {
        pageIdx = freePages.top();
        freePages.pop();
    }
    data = bm->pinWithoutReadingFromFile(*fh, pageIdx);

    auto blockHandle = make_unique<BMBackedMemoryBlock>(pageIdx, data);
    if (initializeToZero) {
        memset(blockHandle->data, 0, LARGE_PAGE_SIZE);
    }

    return blockHandle;
}

void MemoryManager::freeBMBackedBlock(uint32_t pageIdx) {
    lock_guard<mutex> lock(memMgrLock);
    bm->unpin(*fh, pageIdx);
    freePages.push(pageIdx);
}

unique_ptr<OSBackedMemoryBlock> MemoryManager::allocateOSBackedBlock(
    uint64_t size, bool initializeToZero) {
    lock_guard<mutex> lock(memMgrLock);

    auto blockHandle = make_unique<OSBackedMemoryBlock>(size);
    if (initializeToZero) {
        memset(blockHandle->data, 0, size);
    }

    return blockHandle;
}
} // namespace common
} // namespace graphflow
