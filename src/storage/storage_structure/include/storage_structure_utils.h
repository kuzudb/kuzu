#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

#include "src/common/types/include/types.h"
#include "src/storage/buffer_manager/include/buffer_manager.h"
#include "src/storage/buffer_manager/include/file_handle.h"
#include "src/storage/buffer_manager/include/versioned_file_handle.h"
#include "src/storage/wal/include/wal.h"

namespace graphflow {
namespace storage {

struct WALPageIdxAndFrame {
    WALPageIdxAndFrame(page_idx_t originalPageIdx, page_idx_t pageIdxInWAL, uint8_t* frame)
        : originalPageIdx{originalPageIdx}, pageIdxInWAL{pageIdxInWAL}, frame{frame} {}

    WALPageIdxAndFrame(WALPageIdxAndFrame& other)
        : originalPageIdx{other.originalPageIdx},
          pageIdxInWAL{other.pageIdxInWAL}, frame{other.frame} {}

    page_idx_t originalPageIdx;
    page_idx_t pageIdxInWAL;
    uint8_t* frame;
};

struct WALPageIdxPosInPageAndFrame : WALPageIdxAndFrame {
    WALPageIdxPosInPageAndFrame(WALPageIdxAndFrame walPageIdxAndFrame, uint16_t posInPage)
        : WALPageIdxAndFrame(walPageIdxAndFrame), posInPage{posInPage} {}

    uint16_t posInPage;
};

class StorageStructureUtils {
public:
    constexpr static page_idx_t NULL_PAGE_IDX = PAGE_IDX_MAX;
    constexpr static uint32_t NULL_CHUNK_OR_LARGE_LIST_HEAD_IDX = UINT32_MAX;

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

    static WALPageIdxAndFrame createWALVersionIfNecessaryAndPinPage(page_idx_t originalPageIdx,
        bool insertingNewPage, VersionedFileHandle& fileHandle, BufferManager& bufferManager,
        WAL& wal);

    static void readWALVersionOfPage(VersionedFileHandle& fileHandle, page_idx_t originalPageIdx,
        BufferManager& bufferManager, WAL& wal, const std::function<void(uint8_t*)>& readOp);

    // Note: This function updates a page "transactionally", i.e., creates the WAL version of the
    // page if it doesn't exist. For the original page to be updated, the current WRITE trx needs to
    // commit and checkpoint.
    static void updatePage(VersionedFileHandle& fileHandle, page_idx_t originalPageIdx,
        bool isInsertingNewPage, BufferManager& bufferManager, WAL& wal,
        const std::function<void(uint8_t*)>& updateOp);

    // Unpins the WAL version of a page that was updated and releases the lock of the page (recall
    // we use the same lock to do operations on both the original and WAL versions of the page).
    static void unpinWALPageAndReleaseOriginalPageLock(WALPageIdxAndFrame& walPageIdxAndFrame,
        VersionedFileHandle& fileHandle, BufferManager& bufferManager, WAL& wal);

private:
    static void unpinPageIdxInWALAndReleaseOriginalPageLock(page_idx_t pageIdxInWAL,
        page_idx_t originalPageIdx, VersionedFileHandle& fileHandle, BufferManager& bufferManager,
        WAL& wal);
};
} // namespace storage
} // namespace graphflow
