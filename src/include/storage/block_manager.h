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

class PageChunkManager;

struct PageChunkEntry {
    PageChunkEntry(common::page_idx_t startPageIdx, common::page_idx_t numPages)
        : startPageIdx(startPageIdx), numPages(numPages) {}
    PageChunkEntry(const PageChunkEntry& o, common::page_idx_t startPageInOther)
        : PageChunkEntry(o.startPageIdx + startPageInOther, o.numPages - startPageInOther) {
        KU_ASSERT(startPageInOther <= o.numPages);
    }

    common::page_idx_t startPageIdx;
    common::page_idx_t numPages;
};

struct AllocatedBlockEntry {
    AllocatedBlockEntry(common::page_idx_t startPageIdx, common::page_idx_t numPages,
        PageChunkManager& pageChunkManager)
        : entry(startPageIdx, numPages), pageChunkManager(pageChunkManager) {}
    AllocatedBlockEntry(const PageChunkEntry& entry, PageChunkManager& pageChunkManager)
        : entry(entry), pageChunkManager(pageChunkManager) {}
    AllocatedBlockEntry(const AllocatedBlockEntry& entry, common::page_idx_t startPageInEntry)
        : entry(entry.entry, startPageInEntry), pageChunkManager(entry.pageChunkManager) {}
    PageChunkEntry entry;
    PageChunkManager& pageChunkManager;

    common::page_idx_t getStartPageIdx() const { return entry.startPageIdx; }
    common::page_idx_t getNumPages() const { return entry.numPages; }

    void writePagesToFile(const uint8_t* buffer, uint64_t bufferSize);
    void writePageToFile(const uint8_t* pageBuffer, common::page_idx_t pageOffset);
    bool isInMemoryMode() const;
};

class PageChunkManager {
public:
    PageChunkManager(FileHandle* dataFH, ShadowFile* shadowFile,
        std::unique_ptr<FreeSpaceManager> fsm)
        : freeSpaceManager(std::move(fsm)), dataFH(dataFH), shadowFile(shadowFile) {}
    PageChunkManager(FileHandle* dataFH, ShadowFile* shadowFile)
        : PageChunkManager(dataFH, shadowFile, std::make_unique<FreeSpaceManager>()) {}

    AllocatedBlockEntry allocateBlock(common::page_idx_t numPages);
    void freeBlock(PageChunkEntry block);
    void addUncheckpointedFreeBlock(PageChunkEntry block);

    // returns true if a new page was appended to the shadow file
    bool updateShadowedPageWithCursor(PageCursor cursor, DBFileID dbFileID,
        const std::function<void(uint8_t*, common::offset_t)>& writeOp) const;
    std::pair<FileHandle*, common::page_idx_t> getShadowedFileHandleAndPhysicalPageIdxToPin(
        common::page_idx_t pageIdx, transaction::TransactionType trxType) const;

    void serialize(common::Serializer& serializer);
    static std::unique_ptr<PageChunkManager> deserialize(common::Deserializer& deSer,
        FileHandle* dataFH, ShadowFile* shadowFile);
    void finalizeCheckpoint();

    common::row_idx_t getNumFreeEntries() const { return freeSpaceManager->getNumEntries(); }
    std::vector<PageChunkEntry> getFreeEntries(common::row_idx_t startOffset,
        common::row_idx_t endOffset) const {
        return freeSpaceManager->getEntries(startOffset, endOffset);
    }

private:
    friend AllocatedBlockEntry;

    std::unique_ptr<FreeSpaceManager> freeSpaceManager;
    std::mutex mtx;
    FileHandle* dataFH;
    ShadowFile* shadowFile;
};
} // namespace storage
} // namespace kuzu
