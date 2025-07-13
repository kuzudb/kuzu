#pragma once

#include "storage/buffer_manager/memory_manager.h"
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
    SpillResult spillToDisk(ColumnChunkData& chunk) const;
    void loadFromDisk(ColumnChunkData& chunk) const;
    // reclaims memory from the next full partitioner group in the set
    // and returns the amount of memory reclaimed
    // If the set is empty, returns zero
    SpillResult claimNextGroup();
    // Must only be used once all chunks have been loaded from disk.
    void clearFile();
    ~Spiller();

private:
    FileHandle* getOrCreateDataFH() const;
    FileHandle* getDataFH() const;

private:
    std::string tmpFilePath;
    BufferManager& bufferManager;
    common::VirtualFileSystem* vfs;
    std::unordered_set<ChunkedNodeGroup*> fullPartitionerGroups;
    std::atomic<FileHandle*> dataFH;
    std::mutex partitionerGroupsMtx;
    mutable std::mutex fileCreationMutex;
};

} // namespace storage
} // namespace kuzu
