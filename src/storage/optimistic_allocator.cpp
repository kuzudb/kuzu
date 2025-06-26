#include "storage/optimistic_allocator.h"

#include "storage/file_handle.h"
#include "storage/page_manager.h"

namespace kuzu::storage {
OptimisticAllocator::OptimisticAllocator(PageManager& pageManager)
    : PageAllocator(pageManager.getDataFH()), pageManager(pageManager),
      inMemoryMode(getDataFH()->isInMemoryMode()) {}

PageRange OptimisticAllocator::allocatePageRange(common::page_idx_t numPages) {
    auto pageRange = pageManager.allocatePageRange(numPages);
    if (!inMemoryMode && numPages > 0) {
        uncommittedAllocatedPages.push_back(pageRange);
    }
    return pageRange;
}

void OptimisticAllocator::freePageRange(PageRange block) {
    pageManager.freePageRange(block);
}

void OptimisticAllocator::rollback() {
    if (!inMemoryMode) {
        for (const auto& entry : uncommittedAllocatedPages) {
            pageManager.freeImmediatelyRewritablePageRange(pageManager.getDataFH(), entry);
        }
        uncommittedAllocatedPages.clear();
    }
}

void OptimisticAllocator::commit() {
    if (!inMemoryMode) {
        uncommittedAllocatedPages.clear();
    }
}
} // namespace kuzu::storage
