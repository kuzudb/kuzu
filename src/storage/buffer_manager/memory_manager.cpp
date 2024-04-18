#include "storage/buffer_manager/memory_manager.h"

#include <cstring>

#include "storage/buffer_manager/buffer_manager.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

MemoryBuffer::MemoryBuffer(MemoryAllocator* allocator, page_idx_t pageIdx, uint8_t* buffer,
    uint64_t size)
    : buffer{buffer}, pageIdx{pageIdx}, allocator{allocator}, size{size} {}

MemoryBuffer::~MemoryBuffer() {
    if (buffer != nullptr) {
        allocator->freeBlock(pageIdx, buffer);
    }
}

MemoryAllocator::MemoryAllocator(BufferManager* bm, VirtualFileSystem* vfs) : bm{bm} {
    pageSize = BufferPoolConstants::PAGE_256KB_SIZE;
    fh = bm->getBMFileHandle("mm-256KB", FileHandle::O_IN_MEM_TEMP_FILE,
        BMFileHandle::FileVersionedType::NON_VERSIONED_FILE, vfs, PAGE_256KB);
}

MemoryAllocator::~MemoryAllocator() = default;

std::unique_ptr<MemoryBuffer> MemoryAllocator::allocateBuffer(bool initializeToZero,
    uint64_t size) {
    if (size > BufferPoolConstants::PAGE_256KB_SIZE) [[unlikely]] {
        auto buffer = malloc(size);
        if (initializeToZero) {
            memset(buffer, 0, size);
        }
        return std::make_unique<MemoryBuffer>(this, INVALID_PAGE_IDX,
            reinterpret_cast<uint8_t*>(buffer), size);
    }
    page_idx_t pageIdx;
    {
        std::scoped_lock<std::mutex> lock(allocatorLock);
        if (freePages.empty()) {
            pageIdx = fh->addNewPage();
        } else {
            pageIdx = freePages.top();
            freePages.pop();
        }
    }
    auto buffer = bm->pin(*fh, pageIdx, BufferManager::PageReadPolicy::DONT_READ_PAGE);
    auto memoryBuffer = std::make_unique<MemoryBuffer>(this, pageIdx, buffer);
    if (initializeToZero) {
        memset(memoryBuffer->buffer, 0, pageSize);
    }
    return memoryBuffer;
}

void MemoryAllocator::freeBlock(page_idx_t pageIdx, uint8_t* buffer) {
    if (pageIdx == INVALID_PAGE_IDX) {
        std::free(buffer);
        return;
    } else {
        bm->unpin(*fh, pageIdx);
        std::unique_lock<std::mutex> lock(allocatorLock);
        freePages.push(pageIdx);
    }
}

} // namespace storage
} // namespace kuzu
