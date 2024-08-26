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
class FileHandle;
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

class ShadowUtils {
public:
    constexpr static common::page_idx_t NULL_PAGE_IDX = common::INVALID_PAGE_IDX;

    // Where possible, updatePage/insertNewPage should be used instead
    static ShadowPageAndFrame createShadowVersionIfNecessaryAndPinPage(
        common::page_idx_t originalPage, bool insertingNewPage, FileHandle& fileHandle,
        DBFileID dbFileID, ShadowFile& shadowFile);

    static std::pair<FileHandle*, common::page_idx_t> getFileHandleAndPhysicalPageIdxToPin(
        FileHandle& fileHandle, common::page_idx_t pageIdx, const ShadowFile& shadowFile,
        transaction::TransactionType trxType);

    static void readShadowVersionOfPage(const FileHandle& fileHandle,
        common::page_idx_t originalPageIdx, const ShadowFile& shadowFile,
        const std::function<void(uint8_t*)>& readOp);

    static common::page_idx_t insertNewPage(
        FileHandle& fileHandle, DBFileID dbFileID, ShadowFile& shadowFile,
        const std::function<void(uint8_t*)>& insertOp = [](uint8_t*) -> void {
            // DO NOTHING.
        });

    // Note: This function updates a page "transactionally", i.e., creates the WAL version of the
    // page if it doesn't exist. For the original page to be updated, the current WRITE trx needs to
    // commit and checkpoint.
    static void updatePage(FileHandle& fileHandle, DBFileID dbFileID,
        common::page_idx_t originalPageIdx, bool isInsertingNewPage, ShadowFile& shadowFile,
        const std::function<void(uint8_t*)>& updateOp);
};
} // namespace storage
} // namespace kuzu
