#include "storage/page_manager.h"

#include "common/uniq_lock.h"
#include "storage/file_handle.h"
#include "storage/shadow_utils.h"

namespace kuzu::storage {
static constexpr bool ENABLE_FSM = true;

PageRange PageManager::allocatePageRange(common::page_idx_t numPages) {
    if constexpr (ENABLE_FSM) {
        common::UniqLock lck{mtx};
        auto allocatedFreeChunk = freeSpaceManager->popFreePages(numPages);
        if (allocatedFreeChunk.has_value()) {
            return {*allocatedFreeChunk};
        }
    }
    auto startPageIdx = fileHandle->addNewPages(numPages);
    KU_ASSERT(fileHandle->getNumPages() >= startPageIdx + numPages);
    return PageRange(startPageIdx, numPages);
}

void PageManager::freePageRange(PageRange entry) {
    if constexpr (ENABLE_FSM) {
        common::UniqLock lck{mtx};
        for (uint64_t i = 0; i < entry.numPages; ++i) {
            const auto pageIdx = entry.startPageIdx + i;
            fileHandle->removePageFromFrameIfNecessary(pageIdx);
        }
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
    freeSpaceManager->finalizeCheckpoint();
}
} // namespace kuzu::storage
