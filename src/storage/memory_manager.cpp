#include "src/storage/include/memory_manager.h"

#include <cstring>

namespace graphflow {
namespace storage {

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

} // namespace storage
} // namespace graphflow
