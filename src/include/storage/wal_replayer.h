#pragma once

#include "catalog/catalog.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/wal/wal.h"
#include "storage/wal/wal_record.h"

namespace spdlog {
class logger;
}

namespace kuzu {
namespace storage {

class StorageManager;

// Note: This class is not thread-safe.
class WALReplayer {
public:
    // This interface is used for recovery only. We always recover the disk files before
    // constructing the storageManager and catalog. So this specialized recovery constructor
    // doesn't take in storageManager and bufferManager.
    explicit WALReplayer(WAL* wal);

    WALReplayer(WAL* wal, StorageManager* storageManager, MemoryManager* memoryManager,
        catalog::Catalog* catalog, bool isCheckpoint);

    void replay();

private:
    void init();
    void replayWALRecord(WALRecord& walRecord);
    void checkpointOrRollbackVersionedFileHandleAndBufferManager(
        const WALRecord& walRecord, const StorageStructureID& storageStructureID);
    void truncateFileIfInsertion(
        BMFileHandle* fileHandle, const PageUpdateOrInsertRecord& pageInsertOrUpdateRecord);
    BMFileHandle* getVersionedFileHandleIfWALVersionAndBMShouldBeCleared(
        const StorageStructureID& storageStructureID);
    std::unique_ptr<catalog::Catalog> getCatalogForRecovery(common::DBFileType dbFileType);

private:
    bool isRecovering;
    bool isCheckpoint; // if true does redo operations; if false does undo operations
    // Warning: Some fields of the storageManager may not yet be initialized if the WALReplayer
    // has been initialized during recovery, i.e., isRecovering=true.
    StorageManager* storageManager;
    BufferManager* bufferManager;
    MemoryManager* memoryManager;
    std::shared_ptr<BMFileHandle> walFileHandle;
    std::unique_ptr<uint8_t[]> pageBuffer;
    std::shared_ptr<spdlog::logger> logger;
    WAL* wal;
    catalog::Catalog* catalog;
};

} // namespace storage
} // namespace kuzu
