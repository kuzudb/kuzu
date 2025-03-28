#include "storage/block_manager.h"

#include "storage/shadow_utils.h"
#include "storage/storage_utils.h"

namespace kuzu::storage {
BlockEntry BlockManager::allocateBlock(common::page_idx_t numPages) {
    auto startPageIdx = dataFH->addNewPages(numPages);
    KU_ASSERT(dataFH->getNumPages() >= startPageIdx + numPages);
    return BlockEntry(startPageIdx, numPages, *this);
}

void BlockManager::freeBlock(BlockEntry) {}

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

void BlockEntry::writePagesToFile(const uint8_t* buffer, uint64_t bufferSize) {
    blockManager.dataFH->writePagesToFile(buffer, bufferSize, startPageIdx);
}
void BlockEntry::writePageToFile(const uint8_t* pageBuffer, common::page_idx_t pageOffset) {
    KU_ASSERT(pageOffset < numPages);
    blockManager.dataFH->writePageToFile(pageBuffer, startPageIdx + pageOffset);
}
bool BlockEntry::isInMemoryMode() const {
    return blockManager.dataFH->isInMemoryMode();
}
} // namespace kuzu::storage
