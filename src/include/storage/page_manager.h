#pragma once

#include <mutex>

#include "common/types/types.h"
#include "storage/free_space_manager.h"
#include "storage/page_range.h"

namespace kuzu {
namespace transaction {
enum class TransactionType : uint8_t;
}
namespace storage {
struct PageCursor;
struct DBFileID;
class PageManager;
class FileHandle;

class PageManager {
public:
    explicit PageManager(FileHandle* fileHandle)
        : freeSpaceManager(std::make_unique<FreeSpaceManager>()), fileHandle(fileHandle),
          version(0) {}

    uint64_t getVersion() const { return version; }
    bool changedSinceLastCheckpoint() const { return version != 0; }
    void resetVersion() { version = 0; }

    PageRange allocatePageRange(common::page_idx_t numPages);
    common::page_idx_t allocatePage() { return allocatePageRange(1).startPageIdx; }
    void freePageRange(PageRange block);
    void freePage(common::page_idx_t pageIdx) { freePageRange(PageRange(pageIdx, 1)); }

    // The page manager must first allocate space for itself so that its serialized version also
    // tracks the pages allocated itself
    // Thus this function also allocates and returns the space for the serialized storage maanger
    common::page_idx_t estimatePagesNeededForSerialize();
    void serialize(common::Serializer& serializer);
    void deserialize(common::Deserializer& deSer);
    void finalizeCheckpoint();
    void rollbackCheckpoint() { freeSpaceManager->rollbackCheckpoint(); }

    common::row_idx_t getNumFreeEntries() const { return freeSpaceManager->getNumEntries(); }
    std::vector<PageRange> getFreeEntries(common::row_idx_t startOffset,
        common::row_idx_t endOffset) const {
        return freeSpaceManager->getEntries(startOffset, endOffset);
    }

private:
    std::unique_ptr<FreeSpaceManager> freeSpaceManager;
    std::mutex mtx;
    FileHandle* fileHandle;
    uint64_t version;
};
} // namespace storage
} // namespace kuzu
