#pragma once

#include "common/types/types.h"
#include "storage/file_handle.h"
#include "storage/free_space_manager.h"
#include "storage/page_chunk_entry.h"

namespace kuzu {
namespace transaction {
enum class TransactionType : uint8_t;
}
namespace storage {
struct PageCursor;
struct DBFileID;
class PageChunkManager;

struct AllocatedPageChunkEntry {
    AllocatedPageChunkEntry(common::page_idx_t startPageIdx, common::page_idx_t numPages,
        FileHandle* fileHandle)
        : entry(startPageIdx, numPages), fileHandle(fileHandle) {}
    AllocatedPageChunkEntry(const PageChunkEntry& entry, FileHandle* fileHandle)
        : entry(entry), fileHandle(fileHandle) {}
    AllocatedPageChunkEntry(const AllocatedPageChunkEntry& entry,
        common::page_idx_t startPageInEntry)
        : entry(entry.entry, startPageInEntry), fileHandle(entry.fileHandle) {}
    PageChunkEntry entry;
    FileHandle* fileHandle;

    common::page_idx_t getStartPageIdx() const { return entry.startPageIdx; }
    common::page_idx_t getNumPages() const { return entry.numPages; }

    void writePagesToFile(const uint8_t* buffer, uint64_t bufferSize) const;
    void writePageToFile(const uint8_t* pageBuffer, common::page_idx_t pageOffset) const;
    bool isInMemoryMode() const;
};

class PageChunkManager {
public:
    PageChunkManager(FileHandle* fileHandle, ShadowFile* shadowFile,
        std::unique_ptr<FreeSpaceManager> fsm)
        : freeSpaceManager(std::move(fsm)), fileHandle(fileHandle), shadowFile(shadowFile) {}
    PageChunkManager(FileHandle* fileHandle, ShadowFile* shadowFile)
        : PageChunkManager(fileHandle, shadowFile, std::make_unique<FreeSpaceManager>()) {}

    AllocatedPageChunkEntry allocateBlock(common::page_idx_t numPages);
    void freeBlock(PageChunkEntry block);

    // returns true if a new page was appended to the shadow file
    bool updateShadowedPageWithCursor(PageCursor cursor, DBFileID dbFileID,
        const std::function<void(uint8_t*, common::offset_t)>& writeOp) const;
    std::pair<FileHandle*, common::page_idx_t> getShadowedFileHandleAndPhysicalPageIdxToPin(
        common::page_idx_t pageIdx, transaction::TransactionType trxType) const;

    void serialize(common::Serializer& serializer);
    static std::unique_ptr<PageChunkManager> deserialize(common::Deserializer& deSer,
        FileHandle* fileHandle, ShadowFile* shadowFile);
    void finalizeCheckpoint();

    common::row_idx_t getNumFreeEntries() const { return freeSpaceManager->getNumEntries(); }
    std::vector<PageChunkEntry> getFreeEntries(common::row_idx_t startOffset,
        common::row_idx_t endOffset) const {
        return freeSpaceManager->getEntries(startOffset, endOffset);
    }

private:
    std::unique_ptr<FreeSpaceManager> freeSpaceManager;
    std::mutex mtx;
    FileHandle* fileHandle;
    ShadowFile* shadowFile;
};
} // namespace storage
} // namespace kuzu
