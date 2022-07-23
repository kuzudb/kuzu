#pragma once

#include "src/storage/buffer_manager/include/buffer_manager.h"
#include "src/storage/buffer_manager/include/file_handle.h"
#include "src/storage/buffer_manager/include/versioned_file_handle.h"
#include "src/storage/wal/include/wal.h"

namespace graphflow {
namespace storage {

struct UpdatedWALPageIdxAndFrame {
    UpdatedWALPageIdxAndFrame(page_idx_t originalPageIdx, page_idx_t pageIdxInWAL, uint8_t* frame)
        : originalPageIdx{originalPageIdx}, pageIdxInWAL{pageIdxInWAL}, frame{frame} {}

    UpdatedWALPageIdxAndFrame(UpdatedWALPageIdxAndFrame& other)
        : originalPageIdx{other.originalPageIdx},
          pageIdxInWAL{other.pageIdxInWAL}, frame{other.frame} {}
    page_idx_t originalPageIdx;
    page_idx_t pageIdxInWAL;
    uint8_t* frame;
};

struct UpdatedWALPageIdxPosInPageAndFrame : UpdatedWALPageIdxAndFrame {
    UpdatedWALPageIdxPosInPageAndFrame(
        UpdatedWALPageIdxAndFrame updatedWALPageIdxAndFrame, uint16_t posInPage)
        : posInPage{posInPage}, UpdatedWALPageIdxAndFrame(updatedWALPageIdxAndFrame) {}

    uint16_t posInPage;
};

class StorageStructureUtils {
public:
    inline static void pinEachPageOfFile(FileHandle& fileHandle, BufferManager& bufferManager) {
        for (auto pageIdx = 0u; pageIdx < fileHandle.getNumPages(); ++pageIdx) {
            bufferManager.pin(fileHandle, pageIdx);
        }
    }

    inline static void unpinEachPageOfFile(FileHandle& fileHandle, BufferManager& bufferManager) {
        for (auto pageIdx = 0u; pageIdx < fileHandle.getNumPages(); ++pageIdx) {
            bufferManager.unpin(fileHandle, pageIdx);
        }
    }

    static pair<FileHandle*, page_idx_t> getFileHandleAndPhysicalPageIdxToPin(
        VersionedFileHandle& fileHandle, page_idx_t physicalPageIdx, WAL& wal, bool isReadTrx);

    static UpdatedWALPageIdxAndFrame createWALVersionOfPageIfNecessary(page_idx_t originalPageIdx,
        bool insertingNewPage, VersionedFileHandle& fileHandle, BufferManager& bufferManager,
        WAL& wal);

    static void readWALVersionOfPage(VersionedFileHandle& fileHandle, page_idx_t originalPageIdx,
        BufferManager& bufferManager, WAL& wal, std::function<void(uint8_t*)> readOp);

    static void updatePageTransactionally(VersionedFileHandle& fileHandle,
        page_idx_t originalPageIdx, bool isInsertingNewPage, BufferManager& bufferManager, WAL& wal,
        std::function<void(uint8_t*)> updateOp);

    // Unpins the WAL version of a page that was updated and releases the lock of the page (recall
    // we use the same lock to do operations on both the original and WAL version of the page).
    static void unpinWALPageAndReleaseOriginalPageLock(
        UpdatedWALPageIdxAndFrame& updatedWALPageIdxAndFrame, VersionedFileHandle& fileHandle,
        BufferManager& bufferManager, WAL& wal);

private:
    static void unpinPageIdxInWALAndReleaseOriginalPageLock(page_idx_t pageIdxInWAL,
        page_idx_t originalPageIdx, VersionedFileHandle& fileHandle, BufferManager& bufferManager,
        WAL& wal);
};
} // namespace storage
} // namespace graphflow
