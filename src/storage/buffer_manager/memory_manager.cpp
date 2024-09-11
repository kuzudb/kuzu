#include "storage/buffer_manager/memory_manager.h"

#include <cstdint>
#include <cstring>

#include "common/constants.h"
#include "common/exception/buffer_manager.h"
#include "storage/buffer_manager/buffer_manager.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

MemoryBuffer::MemoryBuffer(MemoryManager* mm, page_idx_t pageIdx, uint8_t* buffer, uint64_t size)
    : buffer{buffer, size}, pageIdx{pageIdx}, mm{mm} {}

MemoryBuffer::~MemoryBuffer() {
    if (buffer.data() != nullptr) {
        mm->freeBlock(pageIdx, buffer);
        buffer = std::span<uint8_t>();
    }
}

MemoryManager::MemoryManager(BufferManager* bm, VirtualFileSystem* vfs,
    main::ClientContext* context)
    : bm{bm} {
    pageSize = TEMP_PAGE_SIZE;
    fh = bm->getFileHandle("mm-256KB", FileHandle::O_IN_MEM_TEMP_FILE, vfs, context, TEMP_PAGE);
}

MemoryManager::~MemoryManager() = default;

std::unique_ptr<MemoryBuffer> MemoryManager::mallocBuffer(bool initializeToZero, uint64_t size) {
    if (!bm->reserve(size)) {
        throw BufferManagerException(
            "Unable to allocate memory! The buffer pool is full and no memory could be freed!");
    }
    void* buffer;
    if (initializeToZero) {
        buffer = calloc(size, 1);
    } else {
        buffer = malloc(size);
    }
    return std::make_unique<MemoryBuffer>(this, INVALID_PAGE_IDX,
        reinterpret_cast<uint8_t*>(buffer), size);
}

std::unique_ptr<MemoryBuffer> MemoryManager::allocateBuffer(bool initializeToZero, uint64_t size) {
    if (size > TEMP_PAGE_SIZE) [[unlikely]] {
        return mallocBuffer(initializeToZero, size);
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
    auto buffer = bm->pin(*fh, pageIdx, PageReadPolicy::DONT_READ_PAGE);
    auto memoryBuffer = std::make_unique<MemoryBuffer>(this, pageIdx, buffer);
    if (initializeToZero) {
        memset(memoryBuffer->buffer.data(), 0, pageSize);
    }
    return memoryBuffer;
}

void MemoryManager::freeBlock(page_idx_t pageIdx, std::span<uint8_t> buffer) {
    if (pageIdx == INVALID_PAGE_IDX) {
        bm->freeUsedMemory(buffer.size());
        std::free(buffer.data());
        return;
    } else {
        bm->unpin(*fh, pageIdx);
        std::unique_lock<std::mutex> lock(allocatorLock);
        freePages.push(pageIdx);
    }
}

} // namespace storage
} // namespace kuzu
