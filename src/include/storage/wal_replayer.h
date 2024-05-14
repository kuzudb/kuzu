#pragma once

#include "storage/wal/wal_record.h"

namespace kuzu {
namespace main {
class ClientContext;
} // namespace main

namespace storage {

class StorageManager;
class BufferManager;
enum class WALReplayMode : uint8_t { COMMIT_CHECKPOINT, ROLLBACK, RECOVERY_CHECKPOINT };

class WALReplayer {
public:
    WALReplayer(main::ClientContext& clientContext, BMFileHandle& shadowFH,
        WALReplayMode replayMode);

    void replay();

private:
    void replayWALRecord(WALRecord& walRecord,
        std::unordered_map<DBFileID, std::unique_ptr<common::FileInfo>>& fileCache);
    void replayPageUpdateOrInsertRecord(const WALRecord& walRecord,
        std::unordered_map<DBFileID, std::unique_ptr<common::FileInfo>>& fileCache);
    void replayCatalogRecord(const WALRecord& walRecord);
    void replayTableStatisticsRecord(const WALRecord& walRecord);
    void replayCreateCatalogEntryRecord(const WALRecord& walRecord);
    void replayDropCatalogEntryRecord(const WALRecord& walRecord);
    void replayCopyTableRecord(const WALRecord& walRecord) const;

    void checkpointOrRollbackVersionedFileHandleAndBufferManager(const WALRecord& walRecord,
        const DBFileID& dbFileID);
    void truncateFileIfInsertion(BMFileHandle* fileHandle,
        const PageUpdateOrInsertRecord& pageInsertOrUpdateRecord);
    BMFileHandle* getVersionedFileHandleIfWALVersionAndBMShouldBeCleared(const DBFileID& dbFileID);

private:
    std::string walFilePath;
    BMFileHandle& shadowFH;
    bool isRecovering;
    bool isCheckpoint; // if true does redo operations; if false does undo operations
    std::unique_ptr<uint8_t[]> pageBuffer;
    // Warning: Some fields of the storageManager may not yet be initialized if the WALReplayer
    // has been initialized during recovery, i.e., isRecovering=true.
    main::ClientContext& clientContext;
};

} // namespace storage
} // namespace kuzu
