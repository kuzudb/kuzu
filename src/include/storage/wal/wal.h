#pragma once

#include <unordered_set>

#include "storage/buffer_manager/buffer_manager.h"
#include "storage/wal/wal_record.h"

namespace kuzu {
namespace common {
class BufferedFileWriter;
class VirtualFileSystem;
} // namespace common

namespace storage {

using lock_t = std::unique_lock<std::mutex>;
constexpr uint64_t WAL_HEADER_PAGE_SIZE = common::BufferPoolConstants::PAGE_4KB_SIZE;
constexpr uint64_t WAL_HEADER_PAGE_NUM_RECORDS_FIELD_SIZE = sizeof(uint64_t);
constexpr uint64_t WAL_HEADER_PAGE_NEXT_HEADER_PAGE_IDX_FIELD_SIZE = sizeof(common::page_idx_t);
constexpr uint64_t WAL_HEADER_PAGE_PREFIX_FIELD_SIZES =
    WAL_HEADER_PAGE_NUM_RECORDS_FIELD_SIZE + WAL_HEADER_PAGE_NEXT_HEADER_PAGE_IDX_FIELD_SIZE;

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

    void logCommit(uint64_t transactionID);

    void logTableStatisticsRecord(common::TableType tableType);

    void logCatalogRecord();
    void logCreateTableRecord(common::table_id_t tableID, common::TableType tableType);
    void logCreateRdfGraphRecord(common::table_id_t rdfGraphID, common::table_id_t resourceTableID,
        common::table_id_t literalTableID, common::table_id_t resourceTripleTableID,
        common::table_id_t literalTripleTableID);
    void logDropTableRecord(common::table_id_t tableID);
    void logDropPropertyRecord(common::table_id_t tableID, common::property_id_t propertyID);
    void logAddPropertyRecord(common::table_id_t tableID, common::property_id_t propertyID);

    void logCopyTableRecord(common::table_id_t tableID);

    // Removes the contents of WAL file.
    void clearWAL();

    // We might need another way to check that the last record is commit for recovery and then
    // we might remove this for now to reduce our code size.
    inline bool isLastLoggedRecordCommit() {
        lock_t lck{mtx};
        return isLastRecordCommit;
    }

    void flushAllPages();

    bool isEmptyWAL() const;

    // TODO(Guodong): I feel this interface is used in a abused way. Should revisit and clean up.
    inline std::string getDirectory() const { return directory; }

    inline void addToUpdatedTables(common::table_id_t nodeTableID) {
        updatedTables.insert(nodeTableID);
    }
    inline std::unordered_set<common::table_id_t>& getUpdatedTables() { return updatedTables; }
    BMFileHandle& getShadowingFH() { return *shadowingFH; }

private:
    void initialize();
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
    bool isEmpty;
    bool isLastRecordCommit;
};

} // namespace storage
} // namespace kuzu
