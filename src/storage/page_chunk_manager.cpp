#include "storage/page_chunk_manager.h"

#include "common/uniq_lock.h"
#include "storage/shadow_utils.h"
#include "storage/storage_utils.h"

namespace kuzu::storage {
static constexpr bool ENABLE_FSM = true;

AllocatedPageChunkEntry PageChunkManager::allocateBlock(common::page_idx_t numPages) {
    if constexpr (ENABLE_FSM) {
        common::UniqLock lck{mtx};
        auto allocatedFreeChunk = freeSpaceManager->popFreeChunk(numPages);
        if (allocatedFreeChunk.has_value()) {
            return AllocatedPageChunkEntry{*allocatedFreeChunk, fileHandle};
        }
    }
    auto startPageIdx = fileHandle->addNewPages(numPages);
    KU_ASSERT(fileHandle->getNumPages() >= startPageIdx + numPages);
    return AllocatedPageChunkEntry(startPageIdx, numPages, fileHandle);
}

void PageChunkManager::freeBlock(PageChunkEntry entry) {
    if constexpr (ENABLE_FSM) {
        common::UniqLock lck{mtx};
        for (uint64_t i = 0; i < entry.numPages; ++i) {
            const auto pageIdx = entry.startPageIdx + i;
            fileHandle->removePageFromFrameIfNecessary(pageIdx);
        }
        freeSpaceManager->addFreeChunk(entry);
    }
}

bool PageChunkManager::updateShadowedPageWithCursor(PageCursor cursor, DBFileID dbFileID,
    const std::function<void(uint8_t*, common::offset_t)>& writeOp) const {
    bool insertingNewPage = false;
    if (cursor.pageIdx == common::INVALID_PAGE_IDX) {
        writeOp(nullptr, cursor.elemPosInPage);
        return 0;
    }
    if (cursor.pageIdx >= fileHandle->getNumPages()) {
        KU_ASSERT(cursor.pageIdx == fileHandle->getNumPages());
        ShadowUtils::insertNewPage(*fileHandle, dbFileID, *shadowFile);
        insertingNewPage = true;
    }
    ShadowUtils::updatePage(*fileHandle, dbFileID, cursor.pageIdx, insertingNewPage, *shadowFile,
        [&](auto frame) { writeOp(frame, cursor.elemPosInPage); });
    return insertingNewPage;
}

std::pair<FileHandle*, common::page_idx_t>
PageChunkManager::getShadowedFileHandleAndPhysicalPageIdxToPin(common::page_idx_t pageIdx,
    transaction::TransactionType trxType) const {
    return ShadowUtils::getFileHandleAndPhysicalPageIdxToPin(*fileHandle, pageIdx, *shadowFile,
        trxType);
}

void AllocatedPageChunkEntry::writePagesToFile(const uint8_t* buffer, uint64_t bufferSize) const {
    fileHandle->writePagesToFile(buffer, bufferSize, entry.startPageIdx);
}

void AllocatedPageChunkEntry::writePageToFile(const uint8_t* pageBuffer,
    common::page_idx_t pageOffset) const {
    KU_ASSERT(pageOffset < entry.numPages);
    fileHandle->writePageToFile(pageBuffer, entry.startPageIdx + pageOffset);
}

bool AllocatedPageChunkEntry::isInMemoryMode() const {
    return fileHandle->isInMemoryMode();
}

void PageChunkManager::serialize(common::Serializer& serializer) {
    freeSpaceManager->serialize(serializer);
}

std::unique_ptr<PageChunkManager> PageChunkManager::deserialize(common::Deserializer& deSer,
    FileHandle* fileHandle, ShadowFile* shadowFile) {
    return std::make_unique<PageChunkManager>(fileHandle, shadowFile,
        FreeSpaceManager::deserialize(deSer));
}

void PageChunkManager::finalizeCheckpoint() {
    freeSpaceManager->finalizeCheckpoint();
}
} // namespace kuzu::storage
