#include "storage/page_manager.h"

#include "common/uniq_lock.h"
#include "storage/file_handle.h"
#include "storage/shadow_utils.h"

namespace kuzu::storage {
static constexpr bool ENABLE_FSM = true;

PageChunkEntry PageManager::allocateBlock(common::page_idx_t numPages) {
    if constexpr (ENABLE_FSM) {
        common::UniqLock lck{mtx};
        auto allocatedFreeChunk = freeSpaceManager->popFreeChunk(numPages);
        if (allocatedFreeChunk.has_value()) {
            return {*allocatedFreeChunk};
        }
    }
    auto startPageIdx = fileHandle->addNewPages(numPages);
    KU_ASSERT(fileHandle->getNumPages() >= startPageIdx + numPages);
    return PageChunkEntry(startPageIdx, numPages);
}

void PageManager::freeBlock(PageChunkEntry entry) {
    if constexpr (ENABLE_FSM) {
        common::UniqLock lck{mtx};
        for (uint64_t i = 0; i < entry.numPages; ++i) {
            const auto pageIdx = entry.startPageIdx + i;
            fileHandle->removePageFromFrameIfNecessary(pageIdx);
        }
        freeSpaceManager->addFreeChunk(entry);
    }
}

void PageManager::serialize(common::Serializer& serializer) {
    freeSpaceManager->serialize(serializer);
}

std::unique_ptr<PageManager> PageManager::deserialize(common::Deserializer& deSer,
    FileHandle* fileHandle) {
    return std::make_unique<PageManager>(fileHandle, FreeSpaceManager::deserialize(deSer));
}

void PageManager::finalizeCheckpoint() {
    freeSpaceManager->finalizeCheckpoint();
}
} // namespace kuzu::storage
