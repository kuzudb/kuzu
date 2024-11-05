#pragma once

#include <cstdint>

#include "storage/file_handle.h"

namespace kuzu {
namespace common {
class VirtualFileSystem;
};
namespace storage {
class ChunkedNodeGroup;

class BufferManager;
class ColumnChunkData;

// This should only be used with a LocalFileSystem
class Spiller {
public:
    Spiller(std::string tmpFilePath, BufferManager& bufferManager, common::VirtualFileSystem* vfs);
    void addUnusedChunk(ChunkedNodeGroup* nodeGroup);
    void clearUnusedChunk(ChunkedNodeGroup* nodeGroup);
    uint64_t spillToDisk(ColumnChunkData& chunk) const;
    void loadFromDisk(ColumnChunkData& chunk) const;
    // reclaims memory from the next full partitioner group in the set
    // and returns the amount of memory reclaimed
    // If the set is empty, returns zero
    uint64_t claimNextGroup();
    // Must only be used once all chunks have been loaded from disk.
    void clearFile();
    ~Spiller();

private:
    FileHandle* getDataFH() const;

private:
    std::string tmpFilePath;
    BufferManager& bufferManager;
    common::VirtualFileSystem* vfs;
    std::unordered_set<ChunkedNodeGroup*> fullPartitionerGroups;
    FileHandle* dataFH;
    std::mutex partitionerGroupsMtx;
    mutable std::mutex fileCreationMutex;
};

} // namespace storage
} // namespace kuzu
