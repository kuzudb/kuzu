#include "storage/shadow_utils.h"

#include "storage/file_handle.h"
#include "storage/wal/shadow_file.h"
#include "transaction/transaction.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

ShadowPageAndFrame ShadowUtils::createShadowVersionIfNecessaryAndPinPage(page_idx_t originalPage,
    bool insertingNewPage, FileHandle& fileHandle, DBFileID dbFileID, ShadowFile& shadowFile) {
    KU_ASSERT(!fileHandle.isInMemoryMode());
    const auto hasShadowPage = shadowFile.hasShadowPage(fileHandle.getFileIndex(), originalPage);
    auto shadowPage =
        shadowFile.getOrCreateShadowPage(dbFileID, fileHandle.getFileIndex(), originalPage);
    uint8_t* shadowFrame = nullptr;
    try {
        if (hasShadowPage) {
            shadowFrame =
                shadowFile.getShadowingFH().pinPage(shadowPage, PageReadPolicy::READ_PAGE);
        } else {
            shadowFrame =
                shadowFile.getShadowingFH().pinPage(shadowPage, PageReadPolicy::DONT_READ_PAGE);
            if (!insertingNewPage) {
                fileHandle.optimisticReadPage(originalPage,
                    [&](const uint8_t* frame) -> void { memcpy(shadowFrame, frame, PAGE_SIZE); });
            }
        }
        // The shadow page existing already does not mean that it's already dirty
        // It may have been flushed to disk to free memory and then read again
        shadowFile.getShadowingFH().setLockedPageDirty(shadowPage);
    } catch (Exception&) {
        throw;
    }
    return {originalPage, shadowPage, shadowFrame};
}

std::pair<FileHandle*, page_idx_t> ShadowUtils::getFileHandleAndPhysicalPageIdxToPin(
    FileHandle& fileHandle, page_idx_t pageIdx, const ShadowFile& shadowFile,
    transaction::TransactionType trxType) {
    if (trxType == transaction::TransactionType::CHECKPOINT &&
        shadowFile.hasShadowPage(fileHandle.getFileIndex(), pageIdx)) {
        return std::make_pair(&shadowFile.getShadowingFH(),
            shadowFile.getShadowPage(fileHandle.getFileIndex(), pageIdx));
    }
    return std::make_pair(&fileHandle, pageIdx);
}

page_idx_t ShadowUtils::insertNewPage(FileHandle& fileHandle, DBFileID dbFileID,
    ShadowFile& shadowFile, const std::function<void(uint8_t*)>& insertOp) {
    KU_ASSERT(!fileHandle.isInMemoryMode());
    const auto newOriginalPage = fileHandle.addNewPage();
    KU_ASSERT(!shadowFile.hasShadowPage(fileHandle.getFileIndex(), newOriginalPage));
    const auto shadowPage =
        shadowFile.getOrCreateShadowPage(dbFileID, fileHandle.getFileIndex(), newOriginalPage);
    const auto shadowFrame =
        shadowFile.getShadowingFH().pinPage(shadowPage, PageReadPolicy::DONT_READ_PAGE);
    insertOp(shadowFrame);
    shadowFile.getShadowingFH().setLockedPageDirty(shadowPage);
    shadowFile.getShadowingFH().unpinPage(shadowPage);
    return newOriginalPage;
}

void unpinShadowPage(page_idx_t originalPageIdx, page_idx_t shadowPageIdx,
    const ShadowFile& shadowFile) {
    if (originalPageIdx != INVALID_PAGE_IDX) {
        shadowFile.getShadowingFH().unpinPage(shadowPageIdx);
    }
}

void ShadowUtils::updatePage(FileHandle& fileHandle, DBFileID dbFileID, page_idx_t originalPageIdx,
    bool isInsertingNewPage, ShadowFile& shadowFile,
    const std::function<void(uint8_t*)>& updateOp) {
    KU_ASSERT(!fileHandle.isInMemoryMode());
    const auto shadowPageIdxAndFrame = createShadowVersionIfNecessaryAndPinPage(originalPageIdx,
        isInsertingNewPage, fileHandle, dbFileID, shadowFile);
    try {
        updateOp(shadowPageIdxAndFrame.frame);
    } catch (Exception&) {
        unpinShadowPage(shadowPageIdxAndFrame.originalPage, shadowPageIdxAndFrame.shadowPage,
            shadowFile);
        throw;
    }
    unpinShadowPage(shadowPageIdxAndFrame.originalPage, shadowPageIdxAndFrame.shadowPage,
        shadowFile);
}

void ShadowUtils::readShadowVersionOfPage(const FileHandle& fileHandle, page_idx_t originalPageIdx,
    const ShadowFile& shadowFile, const std::function<void(uint8_t*)>& readOp) {
    KU_ASSERT(!fileHandle.isInMemoryMode());
    KU_ASSERT(shadowFile.hasShadowPage(fileHandle.getFileIndex(), originalPageIdx));
    const page_idx_t shadowPageIdx =
        shadowFile.getShadowPage(fileHandle.getFileIndex(), originalPageIdx);
    const auto frame =
        shadowFile.getShadowingFH().pinPage(shadowPageIdx, PageReadPolicy::READ_PAGE);
    readOp(frame);
    unpinShadowPage(shadowPageIdx, originalPageIdx, shadowFile);
}

} // namespace storage
} // namespace kuzu
