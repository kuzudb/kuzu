#include "storage/storage_structure/storage_structure_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::pair<BMFileHandle*, page_idx_t> StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
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

common::page_idx_t StorageStructureUtils::insertNewPage(BMFileHandle& fileHandle,
    StorageStructureID storageStructureID, BufferManager& bufferManager, WAL& wal,
    std::function<void(uint8_t*)> insertOp) {
    auto newOriginalPage = fileHandle.addNewPage();
    auto newWALPage = wal.logPageInsertRecord(storageStructureID, newOriginalPage);
    auto walFrame = bufferManager.pin(
        *wal.fileHandle, newWALPage, BufferManager::PageReadPolicy::DONT_READ_PAGE);
    fileHandle.addWALPageIdxGroupIfNecessary(newOriginalPage);
    fileHandle.setWALPageIdx(newOriginalPage, newWALPage);
    insertOp(walFrame);
    wal.fileHandle->setLockedPageDirty(newWALPage);
    bufferManager.unpin(*wal.fileHandle, newWALPage);
    return newOriginalPage;
}

void StorageStructureUtils::updatePage(BMFileHandle& fileHandle,
    StorageStructureID storageStructureID, page_idx_t originalPageIdx, bool isInsertingNewPage,
    BufferManager& bufferManager, WAL& wal, const std::function<void(uint8_t*)>& updateOp) {
    auto walPageIdxAndFrame = StorageStructureUtils::createWALVersionIfNecessaryAndPinPage(
        originalPageIdx, isInsertingNewPage, fileHandle, storageStructureID, bufferManager, wal);
    updateOp(walPageIdxAndFrame.frame);
    unpinWALPageAndReleaseOriginalPageLock(walPageIdxAndFrame, fileHandle, bufferManager, wal);
}

void StorageStructureUtils::readWALVersionOfPage(BMFileHandle& fileHandle,
    page_idx_t originalPageIdx, BufferManager& bufferManager, WAL& wal,
    const std::function<void(uint8_t*)>& readOp) {
    page_idx_t pageIdxInWAL = fileHandle.getWALPageIdxNoWALPageIdxLock(originalPageIdx);
    auto frame =
        bufferManager.pin(*wal.fileHandle, pageIdxInWAL, BufferManager::PageReadPolicy::READ_PAGE);
    readOp(frame);
    unpinPageIdxInWALAndReleaseOriginalPageLock(
        pageIdxInWAL, originalPageIdx, fileHandle, bufferManager, wal);
}

WALPageIdxAndFrame StorageStructureUtils::createWALVersionIfNecessaryAndPinPage(
    page_idx_t originalPageIdx, bool insertingNewPage, BMFileHandle& fileHandle,
    StorageStructureID storageStructureID, BufferManager& bufferManager, WAL& wal) {
    fileHandle.addWALPageIdxGroupIfNecessary(originalPageIdx);
    page_idx_t pageIdxInWAL;
    uint8_t* walFrame;
    fileHandle.acquireWALPageIdxLock(originalPageIdx);
    if (fileHandle.hasWALPageVersionNoWALPageIdxLock(originalPageIdx)) {
        pageIdxInWAL = fileHandle.getWALPageIdxNoWALPageIdxLock(originalPageIdx);
        walFrame = bufferManager.pin(
            *wal.fileHandle, pageIdxInWAL, BufferManager::PageReadPolicy::READ_PAGE);
    } else {
        pageIdxInWAL = wal.logPageUpdateRecord(
            storageStructureID, originalPageIdx /* pageIdxInOriginalFile */);
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

void StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
    WALPageIdxAndFrame& walPageIdxAndFrame, BMFileHandle& fileHandle, BufferManager& bufferManager,
    WAL& wal) {
    StorageStructureUtils::unpinPageIdxInWALAndReleaseOriginalPageLock(
        walPageIdxAndFrame.pageIdxInWAL, walPageIdxAndFrame.originalPageIdx, fileHandle,
        bufferManager, wal);
}

void StorageStructureUtils::unpinPageIdxInWALAndReleaseOriginalPageLock(page_idx_t pageIdxInWAL,
    page_idx_t originalPageIdx, BMFileHandle& fileHandle, BufferManager& bufferManager, WAL& wal) {
    bufferManager.unpin(*wal.fileHandle, pageIdxInWAL);
    fileHandle.releaseWALPageIdxLock(originalPageIdx);
}
} // namespace storage
} // namespace kuzu
