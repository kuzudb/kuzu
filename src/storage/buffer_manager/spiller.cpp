#include "storage/buffer_manager/spiller.h"

#include <cstdint>

#include "common/assert.h"
#include "common/exception/io.h"
#include "common/file_system/virtual_file_system.h"
#include "common/types/types.h"
#include "storage/file_handle.h"
#include "storage/store/chunked_node_group.h"
#include "storage/store/column_chunk_data.h"

namespace kuzu {
namespace storage {

Spiller::Spiller(const std::string& tmpFilePath, BufferManager& bufferManager,
    common::VirtualFileSystem* vfs)
    // This should only be used with a LocalFileSystem
    : dataFH{bufferManager.getFileHandle(tmpFilePath,
          FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS, vfs, nullptr)} {
    // Clear the file if it already existed (e.g. from a previous run which
    // failed to clean up).
    dataFH->getFileInfo()->truncate(0);
}

void Spiller::addUnusedChunk(ChunkedNodeGroup* nodeGroup) {
    std::unique_lock<std::mutex> lock(partitionerGroupsMtx);
    fullPartitionerGroups.insert(nodeGroup);
}

void Spiller::clearUnusedChunk(ChunkedNodeGroup* nodeGroup) {
    std::unique_lock<std::mutex> lock(partitionerGroupsMtx);
    auto entry = fullPartitionerGroups.find(nodeGroup);
    if (entry != fullPartitionerGroups.end()) {
        fullPartitionerGroups.erase(entry);
    }
}

Spiller::~Spiller() {
    // This should be safe as long as the VFS is always using a local file system and the VFS is
    // destroyed after the buffer manager
    try {
        dataFH->getFileInfo()->truncate(0);
    } catch (common::IOException&) {} // NOLINT
}

uint64_t Spiller::spillToDisk(ColumnChunkData& chunk) const {
    auto& buffer = *chunk.buffer;
    KU_ASSERT(!buffer.evicted);
    auto pageSize = dataFH->getPageSize();
    auto numPages = (buffer.buffer.size_bytes() + pageSize - 1) / pageSize;
    auto startPage = dataFH->addNewPages(numPages);
    dataFH->writePagesToFile(buffer.buffer.data(), buffer.buffer.size_bytes(), startPage);
    buffer.setSpilledToDisk(startPage * pageSize);
    return buffer.buffer.size();
}

void Spiller::loadFromDisk(ColumnChunkData& chunk) const {
    auto& buffer = *chunk.buffer;
    if (buffer.evicted) {
        buffer.prepareLoadFromDisk();
        dataFH->getFileInfo()->readFromFile(buffer.buffer.data(), buffer.buffer.size(),
            buffer.filePosition);
    }
}

uint64_t Spiller::claimNextGroup() {
    ChunkedNodeGroup* groupToFlush = nullptr;
    {
        std::unique_lock<std::mutex> lock(partitionerGroupsMtx);
        if (!fullPartitionerGroups.empty()) {
            auto groupToFlushEntry = fullPartitionerGroups.begin();
            groupToFlush = *groupToFlushEntry;
            fullPartitionerGroups.erase(groupToFlushEntry);
        }
    }
    if (groupToFlush == nullptr) {
        return 0;
    }
    return groupToFlush->spillToDisk();
}

// NOLINTNEXTLINE(readability-make-member-function-const): Function shouldn't be re-ordered
void Spiller::clearFile() {
    dataFH->getFileInfo()->truncate(0);
}
} // namespace storage
} // namespace kuzu
