#pragma once

#include <unordered_set>

#include "common/types/types_include.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/file_handle.h"
#include "storage/wal/wal_record.h"

namespace spdlog {
class logger;
}

namespace kuzu {
namespace storage {

using lock_t = unique_lock<mutex>;
constexpr uint64_t WAL_HEADER_PAGE_SIZE = DEFAULT_PAGE_SIZE;
constexpr uint64_t WAL_HEADER_PAGE_NUM_RECORDS_FIELD_SIZE = sizeof(uint64_t);
constexpr uint64_t WAL_HEADER_PAGE_NEXT_HEADER_PAGE_IDX_FIELD_SIZE = sizeof(page_idx_t);
constexpr uint64_t WAL_HEADER_PAGE_PREFIX_FIELD_SIZES =
    WAL_HEADER_PAGE_NUM_RECORDS_FIELD_SIZE + WAL_HEADER_PAGE_NEXT_HEADER_PAGE_IDX_FIELD_SIZE;

class WALIterator;

class BaseWALAndWALIterator {

protected:
    BaseWALAndWALIterator() : BaseWALAndWALIterator(nullptr) {}

    explicit BaseWALAndWALIterator(shared_ptr<FileHandle> fileHandle)
        : fileHandle{move(fileHandle)}, offsetInCurrentHeaderPage{INT64_MAX}, currentHeaderPageIdx{
                                                                                  INT32_MAX} {
        currentHeaderPageBuffer = make_unique<uint8_t[]>(WAL_HEADER_PAGE_SIZE);
    }

protected:
    inline uint64_t getNumRecordsInCurrentHeaderPage() const {
        return ((uint64_t*)currentHeaderPageBuffer.get())[0];
    }

    inline void incrementNumRecordsInCurrentHeaderPage() const {
        ((uint64_t*)currentHeaderPageBuffer.get())[0]++;
    }

    inline page_idx_t getNextHeaderPageOfCurrentHeaderPage() const {
        return *(
            page_idx_t*)(currentHeaderPageBuffer.get() + WAL_HEADER_PAGE_NUM_RECORDS_FIELD_SIZE);
    }

    inline void setNextHeaderPageOfCurrentHeaderPage(page_idx_t nextHeaderPageIdx) const {
        ((page_idx_t*)(currentHeaderPageBuffer.get() + WAL_HEADER_PAGE_NUM_RECORDS_FIELD_SIZE))[0] =
            nextHeaderPageIdx;
    }

    inline void resetCurrentHeaderPagePrefix() {
        ((uint64_t*)currentHeaderPageBuffer.get())[0] = 0;
        setNextHeaderPageOfCurrentHeaderPage(
            UINT32_MAX); // set next page pageIdx to UINT32_MAX as null
        offsetInCurrentHeaderPage = WAL_HEADER_PAGE_PREFIX_FIELD_SIZES;
    }

public:
    shared_ptr<FileHandle> fileHandle;
    // Used by WAL as the next offset to write and by WALIterator as the next offset to read
    uint64_t offsetInCurrentHeaderPage;
    // First header page of the WAL, if it exists, is always located at page 0 of the WAL.
    page_idx_t currentHeaderPageIdx;
    unique_ptr<uint8_t[]> currentHeaderPageBuffer;
};

class WAL : public BaseWALAndWALIterator {
    friend WALIterator;

public:
    WAL(const string& directory, BufferManager& bufferManager);

    // Destructing WAL flushes any unwritten header page but not the other pages. The caller
    // which possibly has access to the buffer manager needs to ensure any unwritten pages
    // are also flushed to disk.
    ~WAL() {
        lock_t lck{mtx};
        // WAL only keeps track of the current header page. Any prior header pages are already
        // written to disk. So we only flush the current header page.
        flushHeaderPages();
    }

    inline unique_ptr<WALIterator> getIterator() {
        lock_t lck{mtx};
        flushHeaderPages();
        return make_unique<WALIterator>(fileHandle, mtx);
    }

    page_idx_t logPageUpdateRecord(
        StorageStructureID storageStructureID, page_idx_t pageIdxInOriginalFile);

    page_idx_t logPageInsertRecord(
        StorageStructureID storageStructureID, page_idx_t pageIdxInOriginalFile);

    void logCommit(uint64_t transactionID);

    void logTableStatisticsRecord(bool isNodeTable);

    void logCatalogRecord();

    void logNodeTableRecord(table_id_t tableID);

    void logRelTableRecord(table_id_t tableID);

    void logOverflowFileNextBytePosRecord(
        StorageStructureID storageStructureID, uint64_t prevNextByteToWriteTo);

    void logCopyNodeCSVRecord(table_id_t tableID);

    void logCopyRelCSVRecord(table_id_t tableID);

    void logDropTableRecord(bool isNodeTable, table_id_t tableID);

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

    inline static shared_ptr<FileHandle> createWALFileHandle(const string& directory) {
        return make_shared<FileHandle>(
            FileUtils::joinPath(directory, string(StorageConfig::WAL_FILE_SUFFIX)),
            FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS);
    }

    inline string getDirectory() const { return directory; }

    inline void addToUpdatedNodeTables(table_id_t nodeTableID) {
        updatedNodeTables.insert(nodeTableID);
    }

    inline void addToUpdatedRelTables(table_id_t relTableID) {
        updatedRelTables.insert(relTableID);
    }

private:
    inline void flushHeaderPages() {
        if (!isEmptyWAL()) {
            fileHandle->writePage(currentHeaderPageBuffer.get(), currentHeaderPageIdx);
        }
    }

    void initCurrentPage();
    void addNewWALRecordNoLock(WALRecord& walRecord);
    void setIsLastRecordCommit();

public:
    // Node/Rel tables that might have changes to their in-memory data structures that need to be
    // committed/rolled back accordingly during the wal replaying.
    unordered_set<table_id_t> updatedNodeTables;
    unordered_set<table_id_t> updatedRelTables;

private:
    shared_ptr<spdlog::logger> logger;
    string directory;
    mutex mtx;
    BufferManager& bufferManager;
    bool isLastLoggedRecordCommit_;
};

class WALIterator : public BaseWALAndWALIterator {
public:
    explicit WALIterator(shared_ptr<FileHandle> fileHandle, mutex& mtx);

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
    mutex& mtx;
    uint64_t numRecordsReadInCurrentHeaderPage;
};

} // namespace storage
} // namespace kuzu
