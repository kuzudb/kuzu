#pragma once

#include <mutex>

#include "common/types/types.h"
#include "storage/free_space_manager.h"
#include "storage/page_chunk_entry.h"

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
    PageManager(FileHandle* fileHandle, std::unique_ptr<FreeSpaceManager> fsm)
        : freeSpaceManager(std::move(fsm)), fileHandle(fileHandle) {}
    explicit PageManager(FileHandle* fileHandle)
        : PageManager(fileHandle, std::make_unique<FreeSpaceManager>()) {}

    PageChunkEntry allocateBlock(common::page_idx_t numPages);
    void freeBlock(PageChunkEntry block);

    void serialize(common::Serializer& serializer);
    static std::unique_ptr<PageManager> deserialize(common::Deserializer& deSer,
        FileHandle* fileHandle);
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
};
} // namespace storage
} // namespace kuzu
