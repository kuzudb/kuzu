#pragma once

#include "src/catalog/include/catalog.h"
#include "src/storage/buffer_manager/include/buffer_manager.h"
#include "src/storage/buffer_manager/include/versioned_file_handle.h"
#include "src/storage/wal/include/wal.h"
#include "src/storage/wal/include/wal_record.h"

namespace spdlog {
class logger;
}

namespace graphflow {
namespace storage {

class StorageManager;

class WALReplayer {
public:
    WALReplayer(StorageManager* storageManager);

    WALReplayer(StorageManager* storageManager, BufferManager* bufferManager, bool isCheckpoint);

    void replay();

private:
    void init();
    void replayWALRecord(WALRecord& walRecord);
    void checkpointOrRollbackInMemoryColumn(
        const WALRecord& walRecord, const StorageStructureID& storageStructureID);
    void truncateFileIfInsertion(
        VersionedFileHandle* fileHandle, const PageUpdateOrInsertRecord& pageInsertOrUpdateRecord);

private:
    bool isRecovering;
    bool isCheckpoint; // if true does redo operations; if false does undo operations
    // Warning: Some fields of the storageManager may not yet be initialized if the WALReplayer
    // has been initialized during recovery, i.e., isRecovering=true.
    StorageManager* storageManager;
    BufferManager* bufferManager;
    shared_ptr<FileHandle> walFileHandle;
    unique_ptr<uint8_t[]> pageBuffer;
    unordered_set<ListFileID, ListFileIDHasher> fileIDsofListsToCheckpointOrRollback;

    shared_ptr<spdlog::logger> logger;
};

} // namespace storage
} // namespace graphflow
