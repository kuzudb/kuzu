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
    uint8_t* frame;
    fileHandle.acquireWALPageIdxLock(originalPageIdx);
    if (fileHandle.hasWALPageVersionNoWALPageIdxLock(originalPageIdx)) {
        pageIdxInWAL = fileHandle.getWALPageIdxNoWALPageIdxLock(originalPageIdx);
        frame = bufferManager.pin(
            *wal.fileHandle, pageIdxInWAL, BufferManager::PageReadPolicy::READ_PAGE);
    } else {
        pageIdxInWAL = wal.logPageUpdateRecord(
            storageStructureID, originalPageIdx /* pageIdxInOriginalFile */);
        frame = bufferManager.pin(
            *wal.fileHandle, pageIdxInWAL, BufferManager::PageReadPolicy::DONT_READ_PAGE);
        auto originalFrame = bufferManager.pin(fileHandle, originalPageIdx,
            insertingNewPage ? BufferManager::PageReadPolicy::DONT_READ_PAGE :
                               BufferManager::PageReadPolicy::READ_PAGE);
        // Note: This logic only works for db files with DEFAULT_PAGE_SIZEs.
        memcpy(frame, originalFrame, BufferPoolConstants::PAGE_4KB_SIZE);
        bufferManager.unpin(fileHandle, originalPageIdx);
        fileHandle.setWALPageIdxNoLock(originalPageIdx /* pageIdxInOriginalFile */, pageIdxInWAL);
        bufferManager.setPinnedPageDirty(*wal.fileHandle, pageIdxInWAL);
    }
    return {originalPageIdx, pageIdxInWAL, frame};
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
