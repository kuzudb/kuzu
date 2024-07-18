#pragma once

#include "storage/wal/wal_record.h"

namespace kuzu {
namespace main {
class ClientContext;
} // namespace main

namespace storage {

class StorageManager;
class BufferManager;
class BMFileHandle;
enum class WALReplayMode : uint8_t { COMMIT_CHECKPOINT, ROLLBACK, RECOVERY_CHECKPOINT };

class WALReplayer {
public:
    WALReplayer(main::ClientContext& clientContext, WALReplayMode replayMode);

    void replay() const;

private:
    void replayWALRecord(const WALRecord& walRecord) const;
    void replayCreateCatalogEntryRecord(const WALRecord& walRecord) const;
    void replayDropCatalogEntryRecord(const WALRecord& walRecord) const;
    void replayAlterTableEntryRecord(const WALRecord& walRecord) const;
    void replayTableInsertionRecord(const WALRecord& walRecord) const;
    void replayCopyTableRecord(const WALRecord& walRecord) const;
    void replayUpdateSequenceRecord(const WALRecord& walRecord) const;

private:
    std::string walFilePath;
    bool isRecovering;
    bool isCheckpoint; // if true does redo operations; if false does undo operations
    std::unique_ptr<uint8_t[]> pageBuffer;
    // Warning: Some fields of the storageManager may not yet be initialized if the WALReplayer
    // has been initialized during recovery, i.e., isRecovering=true.
    main::ClientContext& clientContext;
};

} // namespace storage
} // namespace kuzu
