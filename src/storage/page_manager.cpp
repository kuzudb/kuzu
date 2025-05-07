#include "storage/page_manager.h"

#include "common/uniq_lock.h"
#include "storage/file_handle.h"
#include "storage/shadow_utils.h"

namespace kuzu::storage {
static constexpr bool ENABLE_FSM = true;

PageRange PageManager::allocatePageRange(common::page_idx_t numPages, bool reclaimOnRollback) {
    common::UniqLock lck{mtx};
    if constexpr (ENABLE_FSM) {
        auto allocatedFreeChunk = freeSpaceManager->popFreePages(numPages);
        if (allocatedFreeChunk.has_value()) {
            pushUncommittedAllocatedPages(*allocatedFreeChunk, reclaimOnRollback);
            return {*allocatedFreeChunk};
        }
    }
    auto startPageIdx = fileHandle->addNewPages(numPages);
    KU_ASSERT(fileHandle->getNumPages() >= startPageIdx + numPages);
    auto ret = PageRange(startPageIdx, numPages);
    pushUncommittedAllocatedPages(ret, reclaimOnRollback);
    return ret;
}

void PageManager::commit() {
    common::UniqLock lck{mtx};
    uncommittedAllocatedPages.clear();
}

/**
 * To rollback we need to do two things:
 * 1. Free any pages that have been allocated since the last commit/checkpoint
 * 2. Any pages marked as freed but not checkpointed (e.g. from checkpointing dropped tables) are
 * no longer freed
 */
void PageManager::rollback() {
    common::UniqLock lck(mtx);
    if constexpr (ENABLE_FSM) {
        // evict pages before they're added to the free list
        for (const auto& entry : uncommittedAllocatedPages) {
            for (uint64_t i = 0; i < entry.numPages; ++i) {
                const auto pageIdx = entry.startPageIdx + i;
                fileHandle->removePageFromFrameIfNecessary(pageIdx);
            }
        }

        // pages from rolled back allocations are immediately reusable
        // we don't truncate the file, instead letting that happen during checkpoint
        for (const auto& pageRange : uncommittedAllocatedPages) {
            freeSpaceManager->addFreePages(pageRange);
        }
    }
    uncommittedAllocatedPages.clear();

    freeSpaceManager->rollback();
}

void PageManager::freePageRange(PageRange entry) {
    if constexpr (ENABLE_FSM) {
        common::UniqLock lck{mtx};
        // Freed pages cannot be immediately reused to ensure checkpoint recovery works
        // Instead they are reusable after the end of the next checkpoint
        freeSpaceManager->addUncheckpointedFreePages(entry);
    }
}

void PageManager::serialize(common::Serializer& serializer) {
    freeSpaceManager->serialize(serializer);
}

void PageManager::deserialize(common::Deserializer& deSer) {
    freeSpaceManager->deserialize(deSer);
}

void PageManager::finalizeCheckpoint() {
    uncommittedAllocatedPages.clear();
    freeSpaceManager->finalizeCheckpoint(fileHandle);
}

void PageManager::pushUncommittedAllocatedPages(PageRange pages, bool reclaimOnRollback) {
    if (reclaimOnRollback && pages.numPages > 0) {
        uncommittedAllocatedPages.push_back(pages);
    }
}

} // namespace kuzu::storage
