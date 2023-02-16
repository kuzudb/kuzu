#include "storage/buffer_manager/memory_manager.h"

#include <cstring>

#include "common/utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

BlockHandle::BlockHandle(
    MemoryAllocator* allocator, common::page_idx_t blockIdx, uint8_t* data, uint64_t pageSize)
    : blockIdx{blockIdx}, data{data}, blockSize{pageSize}, allocator{allocator} {}

BlockHandle::~BlockHandle() {
    if (data != nullptr) {
        allocator->freeBlock(blockIdx);
    }
}

MemoryAllocator::MemoryAllocator(BufferManager* bm, common::PageSizeClass sizeClass) : bm{bm} {
    fh = std::make_unique<FileHandle>("mm-" + Utils::getPageSizeClassString(sizeClass),
        FileHandle::O_IN_MEM_TEMP_FILE, sizeClass);
    pageSize = (uint64_t)1 << sizeClass;
}

MemoryAllocator::~MemoryAllocator() = default;

std::unique_ptr<BlockHandle> MemoryAllocator::allocateBlock(bool initializeToZero) {
    std::lock_guard<std::mutex> lock(allocatorLock);
    page_idx_t pageIdx;
    uint8_t* data;
    if (freePages.empty()) {
        pageIdx = fh->addNewPage();
    } else {
        pageIdx = freePages.top();
        freePages.pop();
    }
    data = bm->pinWithoutReadingFromFile(*fh, pageIdx);

    auto blockHandle = std::make_unique<BlockHandle>(this, pageIdx, data, pageSize);
    if (initializeToZero) {
        memset(blockHandle->data, 0, pageSize);
    }
    return blockHandle;
}

void MemoryAllocator::freeBlock(common::page_idx_t pageIdx) {
    std::lock_guard<std::mutex> lock(allocatorLock);
    bm->unpin(*fh, pageIdx);
    freePages.push(pageIdx);
}

std::unique_ptr<BlockHandle> MemoryManager::allocateBlock(
    common::PageSizeClass sizeClass, bool initializeToZero) {
    auto sizeClassIdx = Utils::getPageSizeClassIdx(sizeClass);
    return allocators[sizeClassIdx]->allocateBlock(initializeToZero);
}

} // namespace storage
} // namespace kuzu
