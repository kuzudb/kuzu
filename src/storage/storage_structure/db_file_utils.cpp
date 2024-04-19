#include "storage/storage_structure/db_file_utils.h"

#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/wal/wal.h"
#include "storage/wal/wal_record.h"
#include "transaction/transaction.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

WALPageIdxAndFrame DBFileUtils::createWALVersionIfNecessaryAndPinPage(page_idx_t originalPageIdx,
    bool insertingNewPage, BMFileHandle& fileHandle, DBFileID dbFileID,
    BufferManager& bufferManager, WAL& wal) {
    fileHandle.addWALPageIdxGroupIfNecessary(originalPageIdx);
    page_idx_t pageIdxInWAL;
    uint8_t* walFrame;
    fileHandle.acquireWALPageIdxLock(originalPageIdx);
    try {
        if (fileHandle.hasWALPageVersionNoWALPageIdxLock(originalPageIdx)) {
            pageIdxInWAL = fileHandle.getWALPageIdxNoWALPageIdxLock(originalPageIdx);
            walFrame = bufferManager.pin(wal.getShadowingFH(), pageIdxInWAL,
                BufferManager::PageReadPolicy::READ_PAGE);
        } else {
            pageIdxInWAL =
                wal.logPageUpdateRecord(dbFileID, originalPageIdx /* pageIdxInOriginalFile */);
            walFrame = bufferManager.pin(wal.getShadowingFH(), pageIdxInWAL,
                BufferManager::PageReadPolicy::DONT_READ_PAGE);
            if (!insertingNewPage) {
                bufferManager.optimisticRead(fileHandle, originalPageIdx,
                    [&](uint8_t* frame) -> void {
                        memcpy(walFrame, frame, BufferPoolConstants::PAGE_4KB_SIZE);
                    });
            }
            fileHandle.setWALPageIdxNoLock(originalPageIdx /* pageIdxInOriginalFile */,
                pageIdxInWAL);
        }
        // The wal page existing already does not mean that it's already dirty
        // It may have been flushed to disk to free memory and then read again
        wal.getShadowingFH().setLockedPageDirty(pageIdxInWAL);
    } catch (Exception& e) {
        fileHandle.releaseWALPageIdxLock(originalPageIdx);
        throw;
    }
    return {originalPageIdx, pageIdxInWAL, walFrame};
}

void unpinPageIdxInWALAndReleaseOriginalPageLock(page_idx_t pageIdxInWAL,
    page_idx_t originalPageIdx, BMFileHandle& fileHandle, BufferManager& bufferManager, WAL& wal) {
    if (originalPageIdx != INVALID_PAGE_IDX) {
        bufferManager.unpin(wal.getShadowingFH(), pageIdxInWAL);
        fileHandle.releaseWALPageIdxLock(originalPageIdx);
    }
}

void unpinWALPageAndReleaseOriginalPageLock(WALPageIdxAndFrame& walPageIdxAndFrame,
    BMFileHandle& fileHandle, BufferManager& bufferManager, WAL& wal) {
    unpinPageIdxInWALAndReleaseOriginalPageLock(walPageIdxAndFrame.pageIdxInWAL,
        walPageIdxAndFrame.originalPageIdx, fileHandle, bufferManager, wal);
}

std::pair<BMFileHandle*, page_idx_t> DBFileUtils::getFileHandleAndPhysicalPageIdxToPin(
    BMFileHandle& fileHandle, page_idx_t physicalPageIdx, WAL& wal,
    transaction::TransactionType trxType) {
    if (trxType == transaction::TransactionType::READ_ONLY ||
        !fileHandle.hasWALPageVersionNoWALPageIdxLock(physicalPageIdx)) {
        return std::make_pair(&fileHandle, physicalPageIdx);
    } else {
        return std::make_pair(&wal.getShadowingFH(),
            fileHandle.getWALPageIdxNoWALPageIdxLock(physicalPageIdx));
    }
}

common::page_idx_t DBFileUtils::insertNewPage(BMFileHandle& fileHandle, DBFileID dbFileID,
    BufferManager& bufferManager, WAL& wal, const std::function<void(uint8_t*)>& insertOp) {
    auto newOriginalPage = fileHandle.addNewPage();
    auto newWALPage = wal.logPageInsertRecord(dbFileID, newOriginalPage);
    auto walFrame = bufferManager.pin(wal.getShadowingFH(), newWALPage,
        BufferManager::PageReadPolicy::DONT_READ_PAGE);
    fileHandle.addWALPageIdxGroupIfNecessary(newOriginalPage);
    fileHandle.setWALPageIdx(newOriginalPage, newWALPage);
    insertOp(walFrame);
    wal.getShadowingFH().setLockedPageDirty(newWALPage);
    bufferManager.unpin(wal.getShadowingFH(), newWALPage);
    return newOriginalPage;
}

void DBFileUtils::updatePage(BMFileHandle& fileHandle, DBFileID dbFileID,
    page_idx_t originalPageIdx, bool isInsertingNewPage, BufferManager& bufferManager, WAL& wal,
    const std::function<void(uint8_t*)>& updateOp) {
    auto walPageIdxAndFrame = createWALVersionIfNecessaryAndPinPage(originalPageIdx,
        isInsertingNewPage, fileHandle, dbFileID, bufferManager, wal);
    try {
        updateOp(walPageIdxAndFrame.frame);
    } catch (Exception& e) {
        unpinWALPageAndReleaseOriginalPageLock(walPageIdxAndFrame, fileHandle, bufferManager, wal);
        throw;
    }
    unpinWALPageAndReleaseOriginalPageLock(walPageIdxAndFrame, fileHandle, bufferManager, wal);
}

void DBFileUtils::readWALVersionOfPage(BMFileHandle& fileHandle, page_idx_t originalPageIdx,
    BufferManager& bufferManager, WAL& wal, const std::function<void(uint8_t*)>& readOp) {
    page_idx_t pageIdxInWAL = fileHandle.getWALPageIdxNoWALPageIdxLock(originalPageIdx);
    auto frame = bufferManager.pin(wal.getShadowingFH(), pageIdxInWAL,
        BufferManager::PageReadPolicy::READ_PAGE);
    readOp(frame);
    unpinPageIdxInWALAndReleaseOriginalPageLock(pageIdxInWAL, originalPageIdx, fileHandle,
        bufferManager, wal);
}

} // namespace storage
} // namespace kuzu
