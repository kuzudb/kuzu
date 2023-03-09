#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>

#include "common/types/types.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/file_handle.h"
#include "storage/buffer_manager/versioned_file_handle.h"
#include "storage/wal/wal.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

struct WALPageIdxAndFrame {
    WALPageIdxAndFrame(
        common::page_idx_t originalPageIdx, common::page_idx_t pageIdxInWAL, uint8_t* frame)
        : originalPageIdx{originalPageIdx}, pageIdxInWAL{pageIdxInWAL}, frame{frame} {}

    WALPageIdxAndFrame(WALPageIdxAndFrame& other)
        : originalPageIdx{other.originalPageIdx},
          pageIdxInWAL{other.pageIdxInWAL}, frame{other.frame} {}

    common::page_idx_t originalPageIdx;
    common::page_idx_t pageIdxInWAL;
    uint8_t* frame;
};

struct WALPageIdxPosInPageAndFrame : WALPageIdxAndFrame {
    WALPageIdxPosInPageAndFrame(WALPageIdxAndFrame walPageIdxAndFrame, uint16_t posInPage)
        : WALPageIdxAndFrame(walPageIdxAndFrame), posInPage{posInPage} {}

    uint16_t posInPage;
};

class StorageStructureUtils {
public:
    constexpr static common::page_idx_t NULL_PAGE_IDX = common::PAGE_IDX_MAX;
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

    static std::pair<FileHandle*, common::page_idx_t> getFileHandleAndPhysicalPageIdxToPin(
        VersionedFileHandle& fileHandle, common::page_idx_t physicalPageIdx, WAL& wal,
        transaction::TransactionType trxType);

    static WALPageIdxAndFrame createWALVersionIfNecessaryAndPinPage(
        common::page_idx_t originalPageIdx, bool insertingNewPage, VersionedFileHandle& fileHandle,
        BufferManager& bufferManager, WAL& wal);

    static void readWALVersionOfPage(VersionedFileHandle& fileHandle,
        common::page_idx_t originalPageIdx, BufferManager& bufferManager, WAL& wal,
        const std::function<void(uint8_t*)>& readOp);

    // Note: This function updates a page "transactionally", i.e., creates the WAL version of the
    // page if it doesn't exist. For the original page to be updated, the current WRITE trx needs to
    // commit and checkpoint.
    static void updatePage(VersionedFileHandle& fileHandle, common::page_idx_t originalPageIdx,
        bool isInsertingNewPage, BufferManager& bufferManager, WAL& wal,
        const std::function<void(uint8_t*)>& updateOp);

    // Unpins the WAL version of a page that was updated and releases the lock of the page (recall
    // we use the same lock to do operations on both the original and WAL versions of the page).
    static void unpinWALPageAndReleaseOriginalPageLock(WALPageIdxAndFrame& walPageIdxAndFrame,
        VersionedFileHandle& fileHandle, BufferManager& bufferManager, WAL& wal);

private:
    static void unpinPageIdxInWALAndReleaseOriginalPageLock(common::page_idx_t pageIdxInWAL,
        common::page_idx_t originalPageIdx, VersionedFileHandle& fileHandle,
        BufferManager& bufferManager, WAL& wal);
};
} // namespace storage
} // namespace kuzu
