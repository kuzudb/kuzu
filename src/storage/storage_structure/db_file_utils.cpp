#include "storage/storage_structure/db_file_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::pair<BMFileHandle*, page_idx_t> DBFileUtils::getFileHandleAndPhysicalPageIdxToPin(
    BMFileHandle& fileHandle, page_idx_t physicalPageIdx, WAL& wal,
    transaction::TransactionType trxType) {
    if (trxType == transaction::TransactionType::READ_ONLY ||
        !fileHandle.hasWALPageVersionNoWALPageIdxLock(physicalPageIdx)) {
        return std::make_pair(&fileHandle, physicalPageIdx);
    } else {
        return std::make_pair(
            wal.fileHandle.get(), fileHandle.getWALPageIdxNoWALPageIdxLock(physicalPageIdx));
    }
}

common::page_idx_t DBFileUtils::insertNewPage(BMFileHandle& fileHandle, DBFileID dbFileID,
    BufferManager& bufferManager, WAL& wal, std::function<void(uint8_t*)> insertOp) {
    auto newOriginalPage = fileHandle.addNewPage();
    auto newWALPage = wal.logPageInsertRecord(dbFileID, newOriginalPage);
    auto walFrame = bufferManager.pin(
        *wal.fileHandle, newWALPage, BufferManager::PageReadPolicy::DONT_READ_PAGE);
    fileHandle.addWALPageIdxGroupIfNecessary(newOriginalPage);
    fileHandle.setWALPageIdx(newOriginalPage, newWALPage);
    insertOp(walFrame);
    wal.fileHandle->setLockedPageDirty(newWALPage);
    bufferManager.unpin(*wal.fileHandle, newWALPage);
    return newOriginalPage;
}

void DBFileUtils::updatePage(BMFileHandle& fileHandle, DBFileID dbFileID,
    page_idx_t originalPageIdx, bool isInsertingNewPage, BufferManager& bufferManager, WAL& wal,
    const std::function<void(uint8_t*)>& updateOp) {
    auto walPageIdxAndFrame = DBFileUtils::createWALVersionIfNecessaryAndPinPage(
        originalPageIdx, isInsertingNewPage, fileHandle, dbFileID, bufferManager, wal);
    updateOp(walPageIdxAndFrame.frame);
    unpinWALPageAndReleaseOriginalPageLock(walPageIdxAndFrame, fileHandle, bufferManager, wal);
}

void DBFileUtils::readWALVersionOfPage(BMFileHandle& fileHandle, page_idx_t originalPageIdx,
    BufferManager& bufferManager, WAL& wal, const std::function<void(uint8_t*)>& readOp) {
    page_idx_t pageIdxInWAL = fileHandle.getWALPageIdxNoWALPageIdxLock(originalPageIdx);
    auto frame =
        bufferManager.pin(*wal.fileHandle, pageIdxInWAL, BufferManager::PageReadPolicy::READ_PAGE);
    readOp(frame);
    unpinPageIdxInWALAndReleaseOriginalPageLock(
        pageIdxInWAL, originalPageIdx, fileHandle, bufferManager, wal);
}

WALPageIdxAndFrame DBFileUtils::createWALVersionIfNecessaryAndPinPage(page_idx_t originalPageIdx,
    bool insertingNewPage, BMFileHandle& fileHandle, DBFileID dbFileID,
    BufferManager& bufferManager, WAL& wal) {
    fileHandle.addWALPageIdxGroupIfNecessary(originalPageIdx);
    page_idx_t pageIdxInWAL;
    uint8_t* walFrame;
    fileHandle.acquireWALPageIdxLock(originalPageIdx);
    if (fileHandle.hasWALPageVersionNoWALPageIdxLock(originalPageIdx)) {
        pageIdxInWAL = fileHandle.getWALPageIdxNoWALPageIdxLock(originalPageIdx);
        walFrame = bufferManager.pin(
            *wal.fileHandle, pageIdxInWAL, BufferManager::PageReadPolicy::READ_PAGE);
    } else {
        pageIdxInWAL =
            wal.logPageUpdateRecord(dbFileID, originalPageIdx /* pageIdxInOriginalFile */);
        walFrame = bufferManager.pin(
            *wal.fileHandle, pageIdxInWAL, BufferManager::PageReadPolicy::DONT_READ_PAGE);
        if (!insertingNewPage) {
            bufferManager.optimisticRead(fileHandle, originalPageIdx, [&](uint8_t* frame) -> void {
                memcpy(walFrame, frame, BufferPoolConstants::PAGE_4KB_SIZE);
            });
        }
        fileHandle.setWALPageIdxNoLock(originalPageIdx /* pageIdxInOriginalFile */, pageIdxInWAL);
        wal.fileHandle->setLockedPageDirty(pageIdxInWAL);
    }
    return {originalPageIdx, pageIdxInWAL, walFrame};
}

void DBFileUtils::unpinWALPageAndReleaseOriginalPageLock(WALPageIdxAndFrame& walPageIdxAndFrame,
    BMFileHandle& fileHandle, BufferManager& bufferManager, WAL& wal) {
    DBFileUtils::unpinPageIdxInWALAndReleaseOriginalPageLock(walPageIdxAndFrame.pageIdxInWAL,
        walPageIdxAndFrame.originalPageIdx, fileHandle, bufferManager, wal);
}

void DBFileUtils::unpinPageIdxInWALAndReleaseOriginalPageLock(page_idx_t pageIdxInWAL,
    page_idx_t originalPageIdx, BMFileHandle& fileHandle, BufferManager& bufferManager, WAL& wal) {
    bufferManager.unpin(*wal.fileHandle, pageIdxInWAL);
    fileHandle.releaseWALPageIdxLock(originalPageIdx);
}
} // namespace storage
} // namespace kuzu
