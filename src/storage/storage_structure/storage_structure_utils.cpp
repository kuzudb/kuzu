#include "src/storage/storage_structure/include/storage_structure_utils.h"

namespace graphflow {
namespace storage {

pair<FileHandle*, page_idx_t> StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
    VersionedFileHandle& fileHandle, page_idx_t physicalPageIdx, WAL& wal, bool isReadTrx) {
    if (isReadTrx || !fileHandle.hasUpdatedWALPageVersionNoPageLock(physicalPageIdx)) {
        return make_pair((FileHandle*)&fileHandle, physicalPageIdx);
    } else {
        return make_pair(
            wal.fileHandle.get(), fileHandle.getUpdatedWALPageVersionNoPageLock(physicalPageIdx));
    }
}

void StorageStructureUtils::updatePageTransactionally(VersionedFileHandle& fileHandle,
    page_idx_t originalPageIdx, bool isInsertingNewPage, BufferManager& bufferManager, WAL& wal,
    std::function<void(uint8_t*)> updateOp) {
    auto updatedWALPageIdxAndFrame = StorageStructureUtils::createWALVersionOfPageIfNecessary(
        originalPageIdx, isInsertingNewPage, fileHandle, bufferManager, wal);
    updateOp(updatedWALPageIdxAndFrame.frame);
    unpinWALPageAndReleaseOriginalPageLock(
        updatedWALPageIdxAndFrame, fileHandle, bufferManager, wal);
}

void StorageStructureUtils::readWALVersionOfPage(VersionedFileHandle& fileHandle,
    page_idx_t originalPageIdx, BufferManager& bufferManager, WAL& wal,
    std::function<void(uint8_t*)> readOp) {
    page_idx_t pageIdxInWAL = fileHandle.getUpdatedWALPageVersionNoPageLock(originalPageIdx);
    auto frame = bufferManager.pinWithoutAcquiringPageLock(
        *wal.fileHandle, pageIdxInWAL, false /* read from file */);
    readOp(frame);
    unpinPageIdxInWALAndReleaseOriginalPageLock(
        pageIdxInWAL, originalPageIdx, fileHandle, bufferManager, wal);
}

UpdatedWALPageIdxAndFrame StorageStructureUtils::createWALVersionOfPageIfNecessary(
    page_idx_t originalPageIdx, bool insertingNewPage, VersionedFileHandle& fileHandle,
    BufferManager& bufferManager, WAL& wal) {
    fileHandle.createPageVersionGroupIfNecessary(originalPageIdx);
    fileHandle.acquirePageLock(originalPageIdx, true /* block */);
    page_idx_t pageIdxInWAL;
    uint8_t* frame;
    if (fileHandle.hasUpdatedWALPageVersionNoPageLock(originalPageIdx)) {
        pageIdxInWAL = fileHandle.getUpdatedWALPageVersionNoPageLock(originalPageIdx);
        frame = bufferManager.pinWithoutAcquiringPageLock(
            *wal.fileHandle, pageIdxInWAL, false /* read from file */);
    } else {
        pageIdxInWAL = wal.logPageUpdateRecord(fileHandle.getStorageStructureIDIDForWALRecord(),
            originalPageIdx /* pageIdxInOriginalFile */);
        frame = bufferManager.pinWithoutAcquiringPageLock(
            *wal.fileHandle, pageIdxInWAL, true /* do not read from file */);
        uint8_t* originalFrame =
            bufferManager.pinWithoutAcquiringPageLock(fileHandle, originalPageIdx,
                insertingNewPage ? true /* do not read from file */ : false /* read from file */);
        // Note: This logic only works for db files with DEFAULT_PAGE_SIZEs.
        memcpy(frame, originalFrame, DEFAULT_PAGE_SIZE);
        bufferManager.unpinWithoutAcquiringPageLock(fileHandle, originalPageIdx);
        fileHandle.setUpdatedWALPageVersionNoLock(
            originalPageIdx /* pageIdxInOriginalFile */, pageIdxInWAL);
        bufferManager.setPinnedPageDirty(*wal.fileHandle, pageIdxInWAL);
    }
    return UpdatedWALPageIdxAndFrame(originalPageIdx, pageIdxInWAL, frame);
}

void StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
    UpdatedWALPageIdxAndFrame& updatedWALPageIdxAndFrame, VersionedFileHandle& fileHandle,
    BufferManager& bufferManager, WAL& wal) {
    StorageStructureUtils::unpinPageIdxInWALAndReleaseOriginalPageLock(
        updatedWALPageIdxAndFrame.pageIdxInWAL, updatedWALPageIdxAndFrame.originalPageIdx,
        fileHandle, bufferManager, wal);
}

void StorageStructureUtils::unpinPageIdxInWALAndReleaseOriginalPageLock(page_idx_t pageIdxInWAL,
    page_idx_t originalPageIdx, VersionedFileHandle& fileHandle, BufferManager& bufferManager,
    WAL& wal) {
    bufferManager.unpinWithoutAcquiringPageLock(*wal.fileHandle, pageIdxInWAL);
    fileHandle.releasePageLock(originalPageIdx);
}
} // namespace storage
} // namespace graphflow