#include "storage/buffer_manager/memory_manager.h"

#include <cstring>

namespace kuzu {
namespace storage {

unique_ptr<MemoryBlock> MemoryManager::allocateBlock(bool initializeToZero) {
    lock_guard<mutex> lock(memMgrLock);
    page_idx_t pageIdx;
    uint8_t* data;
    if (freePages.empty()) {
        pageIdx = fh->addNewPage();
    } else {
        pageIdx = freePages.top();
        freePages.pop();
    }
    data = bm->pinWithoutReadingFromFile(*fh, pageIdx);

    auto blockHandle = make_unique<MemoryBlock>(pageIdx, data);
    if (initializeToZero) {
        memset(blockHandle->data, 0, LARGE_PAGE_SIZE);
    }

    return blockHandle;
}

void MemoryManager::freeBlock(page_idx_t pageIdx) {
    lock_guard<mutex> lock(memMgrLock);
    bm->unpin(*fh, pageIdx);
    freePages.push(pageIdx);
}

} // namespace storage
} // namespace kuzu
