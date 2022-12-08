#pragma once

#include "catalog/catalog.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/versioned_file_handle.h"
#include "storage/wal/wal.h"
#include "storage/wal/wal_record.h"

namespace spdlog {
class logger;
}

namespace kuzu {
namespace storage {

class StorageManager;

class WALReplayer {
public:
    // This interface is used for recovery only. We always recover the disk files before
    // constructing the storageManager and catalog. So this specialized recovery constructor
    // doesn't take in storageManager and bufferManager.
    WALReplayer(WAL* wal);

    WALReplayer(WAL* wal, StorageManager* storageManager, BufferManager* bufferManager,
        MemoryManager* memoryManager, catalog::Catalog* catalog, bool isCheckpoint);

    void replay();

private:
    void init();
    void replayWALRecord(WALRecord& walRecord);
    void checkpointOrRollbackVersionedFileHandleAndBufferManager(
        const WALRecord& walRecord, const StorageStructureID& storageStructureID);
    void truncateFileIfInsertion(
        VersionedFileHandle* fileHandle, const PageUpdateOrInsertRecord& pageInsertOrUpdateRecord);
    VersionedFileHandle* getVersionedFileHandleIfWALVersionAndBMShouldBeCleared(
        const StorageStructureID& storageStructureID);

private:
    bool isRecovering;
    bool isCheckpoint; // if true does redo operations; if false does undo operations
    // Warning: Some fields of the storageManager may not yet be initialized if the WALReplayer
    // has been initialized during recovery, i.e., isRecovering=true.
    StorageManager* storageManager;
    BufferManager* bufferManager;
    MemoryManager* memoryManager;
    shared_ptr<FileHandle> walFileHandle;
    unique_ptr<uint8_t[]> pageBuffer;
    shared_ptr<spdlog::logger> logger;
    WAL* wal;
    catalog::Catalog* catalog;
};

} // namespace storage
} // namespace kuzu
