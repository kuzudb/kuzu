#pragma once

#include "common/types/types.h"
#include "storage/file_handle.h"
#include "storage/free_space_manager.h"
namespace kuzu {
namespace transaction {
enum class TransactionType : uint8_t;
}
namespace storage {
struct PageCursor;
struct DBFileID;

class BlockManager;

struct BlockEntry {
    BlockEntry(common::page_idx_t startPageIdx, common::page_idx_t numPages,
        BlockManager& blockManager)
        : startPageIdx(startPageIdx), numPages(numPages), blockManager(blockManager) {}
    BlockEntry(const BlockEntry& o, common::page_idx_t startPageInOther)
        : BlockEntry(o.startPageIdx + startPageInOther, o.numPages - startPageInOther,
              o.blockManager) {
        KU_ASSERT(startPageInOther <= o.numPages);
    }
    common::page_idx_t getNumPages() const { return numPages; }
    common::page_idx_t getStartPageIdx() const { return startPageIdx; }

    void writePagesToFile(const uint8_t* buffer, uint64_t bufferSize);
    void writePageToFile(const uint8_t* pageBuffer, common::page_idx_t pageOffset);
    bool isInMemoryMode() const;

    common::page_idx_t startPageIdx;
    common::page_idx_t numPages;
    BlockManager& blockManager;
};

class BlockManager {
public:
    BlockManager(FileHandle* dataFH, ShadowFile* shadowFile)
        : dataFH(dataFH), shadowFile(shadowFile) {}

    BlockEntry allocateBlock(common::page_idx_t numPages);
    void freeBlock(BlockEntry block);

    // returns true if a new page was appended to the shadow file
    bool updateShadowedPageWithCursor(PageCursor cursor, DBFileID dbFileID,
        const std::function<void(uint8_t*, common::offset_t)>& writeOp) const;
    std::pair<FileHandle*, common::page_idx_t> getShadowedFileHandleAndPhysicalPageIdxToPin(
        common::page_idx_t pageIdx, transaction::TransactionType trxType) const;

private:
    friend BlockEntry;

    std::unique_ptr<FreeSpaceManager> freeSpaceManager;
    FileHandle* dataFH;
    ShadowFile* shadowFile;
};
} // namespace storage
} // namespace kuzu
