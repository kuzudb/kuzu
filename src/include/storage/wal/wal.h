#pragma once

#include <unordered_set>

#include "common/serializer/buffered_file.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/wal/wal_record.h"

namespace kuzu {
namespace binder {
struct BoundAlterInfo;
} // namespace binder
namespace common {
class BufferedFileWriter;
class VirtualFileSystem;
} // namespace common

namespace catalog {
class CatalogEntry;
struct SequenceChangeData;
} // namespace catalog

namespace storage {

using lock_t = std::unique_lock<std::mutex>;

class WALReplayer;
class WAL {
    friend class WALReplayer;

public:
    WAL(const std::string& directory, bool readOnly, BufferManager& bufferManager,
        common::VirtualFileSystem* vfs, main::ClientContext* context);

    ~WAL();

    void logCreateCatalogEntryRecord(catalog::CatalogEntry* catalogEntry);
    void logDropCatalogEntryRecord(uint64_t entryID, catalog::CatalogEntryType type);
    void logAlterTableEntryRecord(binder::BoundAlterInfo* alterInfo);

    void logCopyTableRecord(common::table_id_t tableID);

    void logUpdateSequenceRecord(common::sequence_id_t sequenceID,
        catalog::SequenceChangeData data);

    void logAndFlushCommit(uint64_t transactionID);
    void logAndFlushCheckpoint();

    // Removes the contents of WAL file.
    void clearWAL();

    void flushAllPages();

    void addToUpdatedTables(const common::table_id_t nodeTableID) {
        updatedTables.insert(nodeTableID);
    }
    std::unordered_set<common::table_id_t>& getUpdatedTables() { return updatedTables; }

    uint64_t getFileSize() const { return bufferedWriter->getFileSize(); }

private:
    void addNewWALRecordNoLock(const WALRecord& walRecord);

private:
    // Keep track of tables that has updates since last checkpoint. Ideally this is used to
    // determine whether the table needs to be checkpointed or not. NOT fully done yet, will rework
    // this later when working on logging and recovery.
    std::unordered_set<common::table_id_t> updatedTables;
    std::unique_ptr<common::FileInfo> fileInfo;
    std::shared_ptr<common::BufferedFileWriter> bufferedWriter;
    std::string directory;
    std::mutex mtx;
    BufferManager& bufferManager;
    common::VirtualFileSystem* vfs;
};

} // namespace storage
} // namespace kuzu
