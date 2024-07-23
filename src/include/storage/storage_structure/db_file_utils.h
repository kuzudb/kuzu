#pragma once

#include <functional>

#include "common/copy_constructors.h"
#include "common/types/types.h"

namespace kuzu {
namespace transaction {
enum class TransactionType : uint8_t;
} // namespace transaction

namespace storage {

struct DBFileID;
class BMFileHandle;
class BufferManager;
class ShadowFile;

struct ShadowPageAndFrame {
    ShadowPageAndFrame(common::page_idx_t originalPageIdx, common::page_idx_t pageIdxInShadow,
        uint8_t* frame)
        : originalPage{originalPageIdx}, shadowPage{pageIdxInShadow}, frame{frame} {}

    DELETE_COPY_DEFAULT_MOVE(ShadowPageAndFrame);

    common::page_idx_t originalPage;
    common::page_idx_t shadowPage;
    uint8_t* frame;
};

class DBFileUtils {
public:
    constexpr static common::page_idx_t NULL_PAGE_IDX = common::INVALID_PAGE_IDX;

    // Where possible, updatePage/insertNewPage should be used instead
    static ShadowPageAndFrame createShadowVersionIfNecessaryAndPinPage(
        common::page_idx_t originalPage, bool insertingNewPage, BMFileHandle& fileHandle,
        DBFileID dbFileID, BufferManager& bufferManager, ShadowFile& shadowFile);

    static std::pair<BMFileHandle*, common::page_idx_t> getFileHandleAndPhysicalPageIdxToPin(
        BMFileHandle& fileHandle, common::page_idx_t pageIdx, const ShadowFile& shadowFile,
        transaction::TransactionType trxType);

    static void readShadowVersionOfPage(const BMFileHandle& fileHandle,
        common::page_idx_t originalPageIdx, BufferManager& bufferManager,
        const ShadowFile& shadowFile, const std::function<void(uint8_t*)>& readOp);

    static common::page_idx_t insertNewPage(
        BMFileHandle& fileHandle, DBFileID dbFileID, BufferManager& bufferManager,
        ShadowFile& shadowFile,
        const std::function<void(uint8_t*)>& insertOp = [](uint8_t*) -> void {
            // DO NOTHING.
        });

    // Note: This function updates a page "transactionally", i.e., creates the WAL version of the
    // page if it doesn't exist. For the original page to be updated, the current WRITE trx needs to
    // commit and checkpoint.
    static void updatePage(BMFileHandle& fileHandle, DBFileID dbFileID,
        common::page_idx_t originalPageIdx, bool isInsertingNewPage, BufferManager& bufferManager,
        ShadowFile& shadowFile, const std::function<void(uint8_t*)>& updateOp);
};
} // namespace storage
} // namespace kuzu
