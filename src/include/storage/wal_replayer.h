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

enum class WALReplayMode : uint8_t { COMMIT_CHECKPOINT, ROLLBACK, RECOVERY_CHECKPOINT };

// Note: This class is not thread-safe.
class WALReplayer {
public:
    WALReplayer(WAL* wal, StorageManager* storageManager, BufferManager* bufferManager,
        catalog::Catalog* catalog, WALReplayMode replayMode);

    void replay();

private:
    void init();
    void replayWALRecord(WALRecord& walRecord);
    void replayPageUpdateOrInsertRecord(const WALRecord& walRecord);
    void replayTableStatisticsRecord(const WALRecord& walRecord);
    void replayCatalogRecord();
    void replayCreateTableRecord(const WALRecord& walRecord);
    void replayRdfGraphRecord(const WALRecord& walRecord);
    void replayOverflowFileNextBytePosRecord(const WALRecord& walRecord);
    void replayCopyTableRecord(const WALRecord& walRecord);
    void replayDropTableRecord(const WALRecord& walRecord);
    void replayDropPropertyRecord(const WALRecord& walRecord);
    void replayAddPropertyRecord(const WALRecord& walRecord);

    void checkpointOrRollbackVersionedFileHandleAndBufferManager(
        const WALRecord& walRecord, const DBFileID& dbFileID);
    void truncateFileIfInsertion(
        BMFileHandle* fileHandle, const PageUpdateOrInsertRecord& pageInsertOrUpdateRecord);
    BMFileHandle* getVersionedFileHandleIfWALVersionAndBMShouldBeCleared(const DBFileID& dbFileID);
    std::unique_ptr<catalog::Catalog> getCatalogForRecovery(common::FileVersionType dbFileType);

private:
    bool isRecovering;
    bool isCheckpoint; // if true does redo operations; if false does undo operations
    // Warning: Some fields of the storageManager may not yet be initialized if the WALReplayer
    // has been initialized during recovery, i.e., isRecovering=true.
    StorageManager* storageManager;
    BufferManager* bufferManager;
    std::shared_ptr<BMFileHandle> walFileHandle;
    std::unique_ptr<uint8_t[]> pageBuffer;
    WAL* wal;
    catalog::Catalog* catalog;
};

} // namespace storage
} // namespace kuzu
