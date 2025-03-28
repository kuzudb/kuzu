#include "storage/block_manager.h"

#include "storage/shadow_utils.h"
#include "storage/storage_utils.h"

namespace kuzu::storage {
BlockEntry BlockManager::allocateBlock(common::page_idx_t numPages) {
    auto startPageIdx = dataFH->addNewPages(numPages);
    return BlockEntry(dataFH, startPageIdx, numPages);
}

void freeBlock(BlockEntry) {}

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
} // namespace kuzu::storage
