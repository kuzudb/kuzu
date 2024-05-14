#pragma once

#include <unordered_set>

#include "storage/buffer_manager/buffer_manager.h"
#include "storage/wal/wal_record.h"

namespace kuzu {
namespace common {
class BufferedFileWriter;
class VirtualFileSystem;
} // namespace common

namespace catalog {
class CatalogEntry;
} // namespace catalog

namespace storage {

using lock_t = std::unique_lock<std::mutex>;

class WALReplayer;
class WAL {
    friend class WALReplayer;

public:
    WAL(const std::string& directory, bool readOnly, BufferManager& bufferManager,
        common::VirtualFileSystem* vfs);

    // Destructing WAL flushes any unwritten header page but not the other pages. The caller
    // which possibly has access to the buffer manager needs to ensure any unwritten pages
    // are also flushed to disk.
    ~WAL();

    common::page_idx_t logPageUpdateRecord(DBFileID dbFileID,
        common::page_idx_t pageIdxInOriginalFile);

    common::page_idx_t logPageInsertRecord(DBFileID dbFileID,
        common::page_idx_t pageIdxInOriginalFile);

    void logCreateCatalogEntryRecord(catalog::CatalogEntry* catalogEntry);
    void logDropTableRecord(common::table_id_t tableID, catalog::CatalogEntryType type);

    void logCopyTableRecord(common::table_id_t tableID);

    void logCreateSequenceRecord(catalog::CatalogEntry* catalogEntry);
    void logDropSequenceRecord(common::sequence_id_t sequenceID);

    void logCatalogRecord();
    void logTableStatisticsRecord(common::TableType tableType);
    void logCommit(uint64_t transactionID);

    // Removes the contents of WAL file.
    void clearWAL();

    void flushAllPages();

    bool isEmptyWAL() const;

    inline void addToUpdatedTables(common::table_id_t nodeTableID) {
        updatedTables.insert(nodeTableID);
    }
    inline std::unordered_set<common::table_id_t>& getUpdatedTables() { return updatedTables; }
    BMFileHandle& getShadowingFH() { return *shadowingFH; }

private:
    void addNewWALRecordNoLock(WALRecord& walRecord);

private:
    // Node/Rel tables that might have changes to their in-memory data structures that need to be
    // committed/rolled back accordingly during the wal replaying.
    // Tables need in memory checkpointing or rolling back.
    std::unordered_set<common::table_id_t> updatedTables;
    std::shared_ptr<common::BufferedFileWriter> bufferedWriter;
    std::unique_ptr<BMFileHandle> shadowingFH;
    std::string directory;
    std::mutex mtx;
    BufferManager& bufferManager;
    common::VirtualFileSystem* vfs;
};

} // namespace storage
} // namespace kuzu
