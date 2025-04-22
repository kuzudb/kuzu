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
        : freeSpaceManager(std::make_unique<FreeSpaceManager>()), fileHandle(fileHandle) {}

    PageRange allocatePageRange(common::page_idx_t numPages);
    void freePageRange(PageRange block);

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
};
} // namespace storage
} // namespace kuzu
