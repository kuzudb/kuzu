#include "storage/buffer_manager/memory_manager.h"

#include <cstring>

using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::unique_ptr<MemoryBlock> MemoryManager::allocateBlock(bool initializeToZero) {
    std::lock_guard<std::mutex> lock(memMgrLock);
    page_idx_t pageIdx;
    uint8_t* data;
    if (freePages.empty()) {
        pageIdx = fh->addNewPage();
    } else {
        pageIdx = freePages.top();
        freePages.pop();
    }
    data = bm->pinWithoutReadingFromFile(*fh, pageIdx);

    auto blockHandle = std::make_unique<MemoryBlock>(pageIdx, data);
    if (initializeToZero) {
        memset(blockHandle->data, 0, LARGE_PAGE_SIZE);
    }

    return blockHandle;
}

void MemoryManager::freeBlock(page_idx_t pageIdx) {
    std::lock_guard<std::mutex> lock(memMgrLock);
    bm->unpin(*fh, pageIdx);
    freePages.push(pageIdx);
}

} // namespace storage
} // namespace kuzu
