#include "storage/storage_structure/storage_structure_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::pair<FileHandle*, page_idx_t> StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
    VersionedFileHandle& fileHandle, page_idx_t physicalPageIdx, WAL& wal,
    transaction::TransactionType trxType) {
    if (trxType == transaction::TransactionType::READ_ONLY ||
        !fileHandle.hasWALPageVersionNoPageLock(physicalPageIdx)) {
        return std::make_pair((FileHandle*)&fileHandle, physicalPageIdx);
    } else {
        return std::make_pair(
            wal.fileHandle.get(), fileHandle.getWALPageVersionNoPageLock(physicalPageIdx));
    }
}

void StorageStructureUtils::updatePage(VersionedFileHandle& fileHandle, page_idx_t originalPageIdx,
    bool isInsertingNewPage, BufferManager& bufferManager, WAL& wal,
    const std::function<void(uint8_t*)>& updateOp) {
    auto walPageIdxAndFrame = StorageStructureUtils::createWALVersionIfNecessaryAndPinPage(
        originalPageIdx, isInsertingNewPage, fileHandle, bufferManager, wal);
    updateOp(walPageIdxAndFrame.frame);
    unpinWALPageAndReleaseOriginalPageLock(walPageIdxAndFrame, fileHandle, bufferManager, wal);
}

void StorageStructureUtils::readWALVersionOfPage(VersionedFileHandle& fileHandle,
    page_idx_t originalPageIdx, BufferManager& bufferManager, WAL& wal,
    const std::function<void(uint8_t*)>& readOp) {
    page_idx_t pageIdxInWAL = fileHandle.getWALPageVersionNoPageLock(originalPageIdx);
    auto frame = bufferManager.pinWithoutAcquiringPageLock(
        *wal.fileHandle, pageIdxInWAL, false /* read from file */);
    readOp(frame);
    unpinPageIdxInWALAndReleaseOriginalPageLock(
        pageIdxInWAL, originalPageIdx, fileHandle, bufferManager, wal);
}

WALPageIdxAndFrame StorageStructureUtils::createWALVersionIfNecessaryAndPinPage(
    page_idx_t originalPageIdx, bool insertingNewPage, VersionedFileHandle& fileHandle,
    BufferManager& bufferManager, WAL& wal) {
    fileHandle.createPageVersionGroupIfNecessary(originalPageIdx);
    fileHandle.acquirePageLock(originalPageIdx, true /* block */);
    page_idx_t pageIdxInWAL;
    uint8_t* frame;
    if (fileHandle.hasWALPageVersionNoPageLock(originalPageIdx)) {
        pageIdxInWAL = fileHandle.getWALPageVersionNoPageLock(originalPageIdx);
        frame = bufferManager.pinWithoutAcquiringPageLock(
            *wal.fileHandle, pageIdxInWAL, false /* read from file */);
    } else {
        pageIdxInWAL = wal.logPageUpdateRecord(fileHandle.getStorageStructureIDIDForWALRecord(),
            originalPageIdx /* pageIdxInOriginalFile */);
        frame = bufferManager.pinWithoutAcquiringPageLock(
            *wal.fileHandle, pageIdxInWAL, true /* do not read from file */);
        uint8_t* originalFrame = bufferManager.pinWithoutAcquiringPageLock(
            fileHandle, originalPageIdx, insertingNewPage);
        // Note: This logic only works for db files with DEFAULT_PAGE_SIZEs.
        memcpy(frame, originalFrame, DEFAULT_PAGE_SIZE);
        bufferManager.unpinWithoutAcquiringPageLock(fileHandle, originalPageIdx);
        fileHandle.setWALPageVersionNoLock(
            originalPageIdx /* pageIdxInOriginalFile */, pageIdxInWAL);
        bufferManager.setPinnedPageDirty(*wal.fileHandle, pageIdxInWAL);
    }
    return WALPageIdxAndFrame(originalPageIdx, pageIdxInWAL, frame);
}

void StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
    WALPageIdxAndFrame& walPageIdxAndFrame, VersionedFileHandle& fileHandle,
    BufferManager& bufferManager, WAL& wal) {
    StorageStructureUtils::unpinPageIdxInWALAndReleaseOriginalPageLock(
        walPageIdxAndFrame.pageIdxInWAL, walPageIdxAndFrame.originalPageIdx, fileHandle,
        bufferManager, wal);
}

void StorageStructureUtils::unpinPageIdxInWALAndReleaseOriginalPageLock(page_idx_t pageIdxInWAL,
    page_idx_t originalPageIdx, VersionedFileHandle& fileHandle, BufferManager& bufferManager,
    WAL& wal) {
    bufferManager.unpinWithoutAcquiringPageLock(*wal.fileHandle, pageIdxInWAL);
    fileHandle.releasePageLock(originalPageIdx);
}
} // namespace storage
} // namespace kuzu
