#pragma once

#include <unordered_set>

#include "storage/buffer_manager/buffer_manager.h"
#include "storage/wal/wal_record.h"

namespace spdlog {
class logger;
}

namespace kuzu {
namespace storage {

using lock_t = std::unique_lock<std::mutex>;
constexpr uint64_t WAL_HEADER_PAGE_SIZE = common::BufferPoolConstants::PAGE_4KB_SIZE;
constexpr uint64_t WAL_HEADER_PAGE_NUM_RECORDS_FIELD_SIZE = sizeof(uint64_t);
constexpr uint64_t WAL_HEADER_PAGE_NEXT_HEADER_PAGE_IDX_FIELD_SIZE = sizeof(common::page_idx_t);
constexpr uint64_t WAL_HEADER_PAGE_PREFIX_FIELD_SIZES =
    WAL_HEADER_PAGE_NUM_RECORDS_FIELD_SIZE + WAL_HEADER_PAGE_NEXT_HEADER_PAGE_IDX_FIELD_SIZE;

class WALIterator;

class BaseWALAndWALIterator {

protected:
    BaseWALAndWALIterator() : BaseWALAndWALIterator(nullptr) {}

    explicit BaseWALAndWALIterator(std::shared_ptr<BMFileHandle> fileHandle)
        : fileHandle{std::move(fileHandle)}, offsetInCurrentHeaderPage{INT64_MAX},
          currentHeaderPageIdx{INT32_MAX} {
        currentHeaderPageBuffer = std::make_unique<uint8_t[]>(WAL_HEADER_PAGE_SIZE);
    }

protected:
    inline uint64_t getNumRecordsInCurrentHeaderPage() const {
        return ((uint64_t*)currentHeaderPageBuffer.get())[0];
    }

    inline void incrementNumRecordsInCurrentHeaderPage() const {
        ((uint64_t*)currentHeaderPageBuffer.get())[0]++;
    }

    inline common::page_idx_t getNextHeaderPageOfCurrentHeaderPage() const {
        return *(common::page_idx_t*)(currentHeaderPageBuffer.get() +
                                      WAL_HEADER_PAGE_NUM_RECORDS_FIELD_SIZE);
    }

    inline void setNextHeaderPageOfCurrentHeaderPage(common::page_idx_t nextHeaderPageIdx) const {
        ((common::page_idx_t*)(currentHeaderPageBuffer.get() +
                               WAL_HEADER_PAGE_NUM_RECORDS_FIELD_SIZE))[0] = nextHeaderPageIdx;
    }

    inline void resetCurrentHeaderPagePrefix() {
        ((uint64_t*)currentHeaderPageBuffer.get())[0] = 0;
        setNextHeaderPageOfCurrentHeaderPage(
            UINT32_MAX); // set next page pageIdx to UINT32_MAX as null
        offsetInCurrentHeaderPage = WAL_HEADER_PAGE_PREFIX_FIELD_SIZES;
    }

public:
    std::shared_ptr<BMFileHandle> fileHandle;
    // Used by WAL as the next offset to write and by WALIterator as the next offset to read
    uint64_t offsetInCurrentHeaderPage;
    // First header page of the WAL, if it exists, is always located at page 0 of the WAL.
    common::page_idx_t currentHeaderPageIdx;
    std::unique_ptr<uint8_t[]> currentHeaderPageBuffer;
};

class WAL : public BaseWALAndWALIterator {
    friend WALIterator;

public:
    WAL(const std::string& directory, bool readOnly, BufferManager& bufferManager);

    // Destructing WAL flushes any unwritten header page but not the other pages. The caller
    // which possibly has access to the buffer manager needs to ensure any unwritten pages
    // are also flushed to disk.
    ~WAL() {
        lock_t lck{mtx};
        // WAL only keeps track of the current header page. Any prior header pages are already
        // written to disk. So we only flush the current header page.
        flushHeaderPages();
    }

    inline std::unique_ptr<WALIterator> getIterator() {
        lock_t lck{mtx};
        flushHeaderPages();
        return make_unique<WALIterator>(fileHandle, mtx);
    }

    common::page_idx_t logPageUpdateRecord(
        DBFileID dbFileID, common::page_idx_t pageIdxInOriginalFile);

    common::page_idx_t logPageInsertRecord(
        DBFileID dbFileID, common::page_idx_t pageIdxInOriginalFile);

    void logCommit(uint64_t transactionID);

    void logTableStatisticsRecord(bool isNodeTable);

    void logCatalogRecord();

    void logCreateNodeTableRecord(common::table_id_t tableID);
    void logCreateRelTableRecord(common::table_id_t tableID);
    void logRdfGraphRecord(common::table_id_t rdfGraphID, common::table_id_t resourceTableID,
        common::table_id_t literalTableID, common::table_id_t resourceTripleTableID,
        common::table_id_t literalTripleTableID);

    void logOverflowFileNextBytePosRecord(DBFileID dbFileID, uint64_t prevNextByteToWriteTo);

    void logCopyTableRecord(common::table_id_t tableID, common::TableType tableType);

    void logDropTableRecord(common::table_id_t tableID);

    void logDropPropertyRecord(common::table_id_t tableID, common::property_id_t propertyID);

    void logAddPropertyRecord(common::table_id_t tableID, common::property_id_t propertyID);

    // Removes the contents of WAL file.
    void clearWAL();

    // We might need another way to check that the last record is commit for recovery and then
    // we might remove this for now to reduce our code size.
    inline bool isLastLoggedRecordCommit() {
        lock_t lck{mtx};
        return isLastLoggedRecordCommit_;
    }

    void flushAllPages();

    inline bool isEmptyWAL() {
        return currentHeaderPageIdx == 0 && (getNumRecordsInCurrentHeaderPage() == 0);
    }

    // TODO(Guodong): I feel this interface is used in a abused way. Should revisit and clean up.
    inline std::string getDirectory() const { return directory; }

    inline void addToUpdatedTables(common::table_id_t nodeTableID) {
        updatedTables.insert(nodeTableID);
    }
    inline std::unordered_set<common::table_id_t>& getUpdatedTables() { return updatedTables; }

private:
    inline void flushHeaderPages() {
        if (!isEmptyWAL()) {
            fileHandle->writePage(currentHeaderPageBuffer.get(), currentHeaderPageIdx);
        }
    }

    void initCurrentPage();
    void addNewWALRecordNoLock(WALRecord& walRecord);
    void setIsLastRecordCommit();

private:
    // Node/Rel tables that might have changes to their in-memory data structures that need to be
    // committed/rolled back accordingly during the wal replaying.
    std::unordered_set<common::table_id_t> updatedTables;
    std::shared_ptr<spdlog::logger> logger;
    std::string directory;
    std::mutex mtx;
    BufferManager& bufferManager;
    bool isLastLoggedRecordCommit_;
};

class WALIterator : public BaseWALAndWALIterator {
public:
    explicit WALIterator(std::shared_ptr<BMFileHandle> fileHandle, std::mutex& mtx);

    inline bool hasNextRecord() {
        lock_t lck{mtx};
        return hasNextRecordNoLock();
    }

    void getNextRecord(WALRecord& retVal);

private:
    inline bool hasNextRecordNoLock() {
        return numRecordsReadInCurrentHeaderPage < getNumRecordsInCurrentHeaderPage();
    }

public:
    std::mutex& mtx;
    uint64_t numRecordsReadInCurrentHeaderPage;
};

} // namespace storage
} // namespace kuzu
