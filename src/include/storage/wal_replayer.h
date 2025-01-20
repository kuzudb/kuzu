#pragma once

#include "storage/wal/wal_record.h"

namespace kuzu {
namespace main {
class ClientContext;
} // namespace main

namespace storage {
class WALReplayer {
public:
    explicit WALReplayer(main::ClientContext& clientContext);

    void replay() const;

private:
    void replayWALRecord(const WALRecord& walRecord) const;
    void replayCreateCatalogEntryRecord(const WALRecord& walRecord) const;
    void replayDropCatalogEntryRecord(const WALRecord& walRecord) const;
    void replayAlterTableEntryRecord(const WALRecord& walRecord) const;
    void replayTableInsertionRecord(const WALRecord& walRecord) const;
    void replayNodeDeletionRecord(const WALRecord& walRecord) const;
    void replayNodeUpdateRecord(const WALRecord& walRecord) const;
    void replayRelDeletionRecord(const WALRecord& walRecord) const;
    void replayRelDetachDeletionRecord(const WALRecord& walRecord) const;
    void replayRelUpdateRecord(const WALRecord& walRecord) const;
    void replayCopyTableRecord(const WALRecord& walRecord) const;
    void replayUpdateSequenceRecord(const WALRecord& walRecord) const;

    void replayNodeTableInsertRecord(const WALRecord& walRecord) const;
    void replayRelTableInsertRecord(const WALRecord& walRecord) const;

private:
    std::string walFilePath;
    std::unique_ptr<uint8_t[]> pageBuffer;
    // Warning: Some fields of the storageManager may not yet be initialized if the WALReplayer
    // has been initialized during recovery, i.e., isRecovering=true.
    main::ClientContext& clientContext;
};

} // namespace storage
} // namespace kuzu
