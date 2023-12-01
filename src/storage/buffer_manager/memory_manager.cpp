#include "storage/buffer_manager/memory_manager.h"

#include <cstring>

#include "storage/buffer_manager/buffer_manager.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

MemoryBuffer::MemoryBuffer(MemoryAllocator* allocator, page_idx_t pageIdx, uint8_t* buffer)
    : buffer{buffer}, pageIdx{pageIdx}, allocator{allocator} {}

MemoryBuffer::~MemoryBuffer() {
    if (buffer != nullptr) {
        allocator->freeBlock(pageIdx);
    }
}

MemoryAllocator::MemoryAllocator(BufferManager* bm) : bm{bm} {
    pageSize = BufferPoolConstants::PAGE_256KB_SIZE;
    fh = bm->getBMFileHandle("mm-256KB", FileHandle::O_IN_MEM_TEMP_FILE,
        BMFileHandle::FileVersionedType::NON_VERSIONED_FILE, PAGE_256KB);
}

MemoryAllocator::~MemoryAllocator() = default;

std::unique_ptr<MemoryBuffer> MemoryAllocator::allocateBuffer(bool initializeToZero) {
    std::unique_lock<std::mutex> lock(allocatorLock);
    page_idx_t pageIdx;
    if (freePages.empty()) {
        pageIdx = fh->addNewPage();
    } else {
        pageIdx = freePages.top();
        freePages.pop();
    }
    auto buffer = bm->pin(*fh, pageIdx, BufferManager::PageReadPolicy::DONT_READ_PAGE);
    auto memoryBuffer = std::make_unique<MemoryBuffer>(this, pageIdx, buffer);
    if (initializeToZero) {
        memset(memoryBuffer->buffer, 0, pageSize);
    }
    return memoryBuffer;
}

void MemoryAllocator::freeBlock(page_idx_t pageIdx) {
    std::unique_lock<std::mutex> lock(allocatorLock);
    bm->unpin(*fh, pageIdx);
    freePages.push(pageIdx);
}

} // namespace storage
} // namespace kuzu
