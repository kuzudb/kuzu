#include "storage/wal/wal.h"

#include "common/exception/runtime.h"
#include "common/utils.h"
#include "spdlog/spdlog.h" // IWYU pragma: keep: public interface to spdlog.
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

WAL::WAL(const std::string& directory, bool readOnly, BufferManager& bufferManager)
    : logger{LoggerUtils::getLogger(LoggerConstants::LoggerEnum::WAL)}, directory{directory},
      bufferManager{bufferManager}, isLastLoggedRecordCommit_{false} {
    fileHandle = bufferManager.getBMFileHandle(
        FileUtils::joinPath(directory, std::string(StorageConstants::WAL_FILE_SUFFIX)),
        readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                   FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::NON_VERSIONED_FILE);
    initCurrentPage();
}

page_idx_t WAL::logPageUpdateRecord(DBFileID dbFileID, page_idx_t pageIdxInOriginalFile) {
    lock_t lck{mtx};
    auto pageIdxInWAL = fileHandle->addNewPage();
    WALRecord walRecord =
        WALRecord::newPageUpdateRecord(dbFileID, pageIdxInOriginalFile, pageIdxInWAL);
    addNewWALRecordNoLock(walRecord);
    return pageIdxInWAL;
}

page_idx_t WAL::logPageInsertRecord(DBFileID dbFileID, page_idx_t pageIdxInOriginalFile) {
    lock_t lck{mtx};
    auto pageIdxInWAL = fileHandle->addNewPage();
    WALRecord walRecord =
        WALRecord::newPageInsertRecord(dbFileID, pageIdxInOriginalFile, pageIdxInWAL);
    addNewWALRecordNoLock(walRecord);
    return pageIdxInWAL;
}

void WAL::logCommit(uint64_t transactionID) {
    lock_t lck{mtx};
    WALRecord walRecord = WALRecord::newCommitRecord(transactionID);
    addNewWALRecordNoLock(walRecord);
}

// TODO(Guodong): Turn the boolean into enum, TableType.
void WAL::logTableStatisticsRecord(bool isNodeTable) {
    lock_t lck{mtx};
    WALRecord walRecord = WALRecord::newTableStatisticsRecord(isNodeTable);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logCatalogRecord() {
    lock_t lck{mtx};
    WALRecord walRecord = WALRecord::newCatalogRecord();
    addNewWALRecordNoLock(walRecord);
}

void WAL::logCreateNodeTableRecord(table_id_t tableID) {
    lock_t lck{mtx};
    WALRecord walRecord = WALRecord::newCreateTableRecord(tableID, TableType::NODE);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logCreateRelTableRecord(table_id_t tableID) {
    lock_t lck{mtx};
    WALRecord walRecord = WALRecord::newCreateTableRecord(tableID, TableType::REL);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logRdfGraphRecord(table_id_t rdfGraphID, table_id_t resourceTableID,
    table_id_t literalTableID, table_id_t resourceTripleTableID, table_id_t literalTripleTableID) {
    lock_t lck{mtx};
    WALRecord walRecord = WALRecord::newRdfGraphRecord(
        rdfGraphID, resourceTableID, literalTableID, resourceTripleTableID, literalTripleTableID);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logOverflowFileNextBytePosRecord(DBFileID dbFileID, uint64_t prevNextByteToWriteTo) {
    lock_t lck{mtx};
    WALRecord walRecord =
        WALRecord::newOverflowFileNextBytePosRecord(dbFileID, prevNextByteToWriteTo);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logCopyTableRecord(table_id_t tableID, TableType tableType) {
    lock_t lck{mtx};
    WALRecord walRecord = WALRecord::newCopyTableRecord(tableID, tableType);
    addToUpdatedTables(tableID);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logDropTableRecord(table_id_t tableID) {
    lock_t lck{mtx};
    WALRecord walRecord = WALRecord::newDropTableRecord(tableID);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logDropPropertyRecord(table_id_t tableID, property_id_t propertyID) {
    lock_t lck{mtx};
    WALRecord walRecord = WALRecord::newDropPropertyRecord(tableID, propertyID);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logAddPropertyRecord(table_id_t tableID, property_id_t propertyID) {
    lock_t lck{mtx};
    WALRecord walRecord = WALRecord::newAddPropertyRecord(tableID, propertyID);
    addNewWALRecordNoLock(walRecord);
}

void WAL::clearWAL() {
    bufferManager.removeFilePagesFromFrames(*fileHandle);
    fileHandle->resetToZeroPagesAndPageCapacity();
    initCurrentPage();
    StorageUtils::removeAllWALFiles(directory);
    updatedTables.clear();
}

void WAL::flushAllPages() {
    if (!isEmptyWAL()) {
        flushHeaderPages();
        bufferManager.flushAllDirtyPagesInFrames(*fileHandle);
    }
}

void WAL::initCurrentPage() {
    currentHeaderPageIdx = 0;
    isLastLoggedRecordCommit_ = false;
    if (fileHandle->getNumPages() == 0) {
        fileHandle->addNewPage();
        resetCurrentHeaderPagePrefix();
    } else {
        // If the file existed, read the first page into the currentHeaderPageBuffer.
        fileHandle->readPage(currentHeaderPageBuffer.get(), 0);
        setIsLastRecordCommit();
    }
}

void WAL::addNewWALRecordNoLock(WALRecord& walRecord) {
    if (offsetInCurrentHeaderPage + sizeof(WALRecord) > WAL_HEADER_PAGE_SIZE) {
        uint64_t nextHeaderPageIdx = fileHandle->addNewPage();
        setNextHeaderPageOfCurrentHeaderPage(nextHeaderPageIdx);
        // We next write the currentHeaderPageBuffer. This allows us to only keep track
        // of only one headerPage as we append more logs but requires us to do more I/O as each
        // header page will be read back from disk when checkpointing. But hopefully the number
        // of header pages is very small. After this write, we can use the
        // currentHeaderPageBuffer as an empty buffer space for the newHeaderPage we just added
        // and which will become the current header page.
        fileHandle->writePage(currentHeaderPageBuffer.get(), currentHeaderPageIdx);
        resetCurrentHeaderPagePrefix();
        currentHeaderPageIdx = nextHeaderPageIdx;
    }
    incrementNumRecordsInCurrentHeaderPage();
    walRecord.writeWALRecordToBytes(currentHeaderPageBuffer.get(), offsetInCurrentHeaderPage);
    isLastLoggedRecordCommit_ = (WALRecordType::COMMIT_RECORD == walRecord.recordType);
}

void WAL::setIsLastRecordCommit() {
    WALIterator walIterator(fileHandle, mtx);
    WALRecord walRecord;
    if (!walIterator.hasNextRecord()) {
        logger->info(
            "Opening an existing WAL file but the file is empty. This should never happen. file: " +
            fileHandle->getFileInfo()->path);
        return;
    }
    while (walIterator.hasNextRecord()) {
        walIterator.getNextRecord(walRecord);
    }
    if (WALRecordType::COMMIT_RECORD == walRecord.recordType) {
        isLastLoggedRecordCommit_ = true;
    }
}

WALIterator::WALIterator(std::shared_ptr<BMFileHandle> fileHandle, std::mutex& mtx)
    : BaseWALAndWALIterator{std::move(fileHandle)}, mtx{mtx} {
    resetCurrentHeaderPagePrefix();
    if (this->fileHandle->getNumPages() > 0) {
        this->fileHandle->readPage(
            currentHeaderPageBuffer.get(), 0 /* first header page is at pageIdx 0 */);
    }
    numRecordsReadInCurrentHeaderPage = 0;
}

void WALIterator::getNextRecord(WALRecord& retVal) {
    lock_t lck{mtx};
    if (!hasNextRecordNoLock()) {
        throw RuntimeException("WALIterator cannot read more log records from the WAL.");
    }
    WALRecord::constructWALRecordFromBytes(
        retVal, currentHeaderPageBuffer.get(), offsetInCurrentHeaderPage);
    numRecordsReadInCurrentHeaderPage++;
    if ((numRecordsReadInCurrentHeaderPage == getNumRecordsInCurrentHeaderPage()) &&
        (getNextHeaderPageOfCurrentHeaderPage() != UINT32_MAX)) {
        page_idx_t nextHeaderPageIdx = getNextHeaderPageOfCurrentHeaderPage();
        fileHandle->readPage(currentHeaderPageBuffer.get(), nextHeaderPageIdx);
        offsetInCurrentHeaderPage = WAL_HEADER_PAGE_PREFIX_FIELD_SIZES;
        numRecordsReadInCurrentHeaderPage = 0;
    }
}

} // namespace storage
} // namespace kuzu
