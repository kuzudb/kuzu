#pragma once

#include <cstdint>
#include <unordered_set>

#include "common/enums/rel_direction.h"
#include "common/serializer/buffered_file.h"
#include "storage/wal/wal_record.h"

namespace kuzu {
namespace binder {
struct BoundAlterInfo;
struct BoundCreateTableInfo;
} // namespace binder
namespace common {
class BufferedFileWriter;
class VirtualFileSystem;
class ValueVector;
} // namespace common

namespace catalog {
class CatalogEntry;
struct SequenceRollbackData;
} // namespace catalog

namespace storage {
class WALReplayer;
class WAL {
    friend class WALReplayer;

public:
    WAL(const std::string& directory, bool readOnly, common::VirtualFileSystem* vfs,
        main::ClientContext* context);

    ~WAL();

    // Currently, only creating a table entry has its own WAL record. Eventually,
    // we want to log minimal info into the WAL, so each entry type should have its
    // own WAL record
    void logCreateCatalogEntryRecord(catalog::CatalogEntry* catalogEntry, bool isInternal);
    void logDropCatalogEntryRecord(uint64_t tableID, catalog::CatalogEntryType type);
    void logAlterCatalogEntryRecord(const binder::BoundAlterInfo* alterInfo);
    void logUpdateSequenceRecord(common::sequence_id_t sequenceID, uint64_t kCount);

    void logTableInsertion(common::table_id_t tableID, common::TableType tableType,
        common::row_idx_t numRows, const std::vector<common::ValueVector*>& vectors);
    void logNodeDeletion(common::table_id_t tableID, common::offset_t nodeOffset,
        common::ValueVector* pkVector);
    void logNodeUpdate(common::table_id_t tableID, common::column_id_t columnID,
        common::offset_t nodeOffset, common::ValueVector* propertyVector);
    void logRelDelete(common::table_id_t tableID, common::ValueVector* srcNodeVector,
        common::ValueVector* dstNodeVector, common::ValueVector* relIDVector);
    void logRelDetachDelete(common::table_id_t tableID, common::RelDataDirection direction,
        common::ValueVector* srcNodeVector);
    void logRelUpdate(common::table_id_t tableID, common::column_id_t columnID,
        common::ValueVector* srcNodeVector, common::ValueVector* dstNodeVector,
        common::ValueVector* relIDVector, common::ValueVector* propertyVector);
    void logCopyTableRecord(common::table_id_t tableID);

    void logBeginTransaction();
    void logAndFlushCommit();
    void logRollback();
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
    common::VirtualFileSystem* vfs;
};

} // namespace storage
} // namespace kuzu
