#include "storage/block_manager.h"

#include "common/uniq_lock.h"
#include "storage/shadow_utils.h"
#include "storage/storage_utils.h"

static constexpr bool ENABLE_FSM = true;

namespace kuzu::storage {
AllocatedBlockEntry BlockManager::allocateBlock(common::page_idx_t numPages) {
    if constexpr (ENABLE_FSM) {
        common::UniqLock lck{mtx};
        // TODO(Royi) check if this is still needed
        // numPages = numPages == 0 ? 0 : std::bit_ceil(numPages);

        auto allocatedFreeChunk = freeSpaceManager->getFreeChunk(numPages);
        if (allocatedFreeChunk.has_value()) {
            return AllocatedBlockEntry{*allocatedFreeChunk, *this};
        }
    }
    auto startPageIdx = dataFH->addNewPages(numPages);
    KU_ASSERT(dataFH->getNumPages() >= startPageIdx + numPages);
    return AllocatedBlockEntry(startPageIdx, numPages, *this);
}

void BlockManager::freeBlock(BlockEntry entry) {
    if constexpr (ENABLE_FSM) {
        common::UniqLock lck{mtx};
        for (uint64_t i = 0; i < entry.numPages; ++i) {
            const auto pageIdx = entry.startPageIdx + i;
            dataFH->removePageFromFrameIfNecessary(pageIdx);
        }
        freeSpaceManager->addFreeChunk(entry);
    }
}

void BlockManager::addUncheckpointedFreeBlock(BlockEntry entry) {
    freeSpaceManager->addUncheckpointedFreeChunk(entry);
}

bool BlockManager::updateShadowedPageWithCursor(PageCursor cursor, DBFileID dbFileID,
    const std::function<void(uint8_t*, common::offset_t)>& writeOp) const {
    bool insertingNewPage = false;
    if (cursor.pageIdx == common::INVALID_PAGE_IDX) {
        writeOp(nullptr, cursor.elemPosInPage);
        return 0;
    }
    if (cursor.pageIdx >= dataFH->getNumPages()) {
        KU_ASSERT(cursor.pageIdx == dataFH->getNumPages());
        ShadowUtils::insertNewPage(*dataFH, dbFileID, *shadowFile);
        insertingNewPage = true;
    }
    ShadowUtils::updatePage(*dataFH, dbFileID, cursor.pageIdx, insertingNewPage, *shadowFile,
        [&](auto frame) { writeOp(frame, cursor.elemPosInPage); });
    return insertingNewPage;
}

std::pair<FileHandle*, common::page_idx_t>
BlockManager::getShadowedFileHandleAndPhysicalPageIdxToPin(common::page_idx_t pageIdx,
    transaction::TransactionType trxType) const {
    return ShadowUtils::getFileHandleAndPhysicalPageIdxToPin(*dataFH, pageIdx, *shadowFile,
        trxType);
}

void AllocatedBlockEntry::writePagesToFile(const uint8_t* buffer, uint64_t bufferSize) {
    blockManager.dataFH->writePagesToFile(buffer, bufferSize, entry.startPageIdx);
}

void AllocatedBlockEntry::writePageToFile(const uint8_t* pageBuffer,
    common::page_idx_t pageOffset) {
    KU_ASSERT(pageOffset < entry.numPages);
    blockManager.dataFH->writePageToFile(pageBuffer, entry.startPageIdx + pageOffset);
}

bool AllocatedBlockEntry::isInMemoryMode() const {
    return blockManager.dataFH->isInMemoryMode();
}

void BlockManager::serialize(common::Serializer& serializer) {
    freeSpaceManager->serialize(serializer);
}

std::unique_ptr<BlockManager> BlockManager::deserialize(common::Deserializer& deSer,
    FileHandle* dataFH, ShadowFile* shadowFile) {
    return std::make_unique<BlockManager>(dataFH, shadowFile, FreeSpaceManager::deserialize(deSer));
}

void BlockManager::finalizeCheckpoint() {
    freeSpaceManager->finalizeCheckpoint();
}
} // namespace kuzu::storage
