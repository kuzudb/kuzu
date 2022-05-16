#include "src/storage/include/wal/wal.h"

#include "spdlog/spdlog.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace storage {

WAL::WAL(const string& path, BufferManager& bufferManager)
    : logger{LoggerUtils::getOrCreateSpdLogger("wal")}, bufferManager{bufferManager},
      isLastLoggedRecordCommit_{false} {
    createFileHandle(path);
    initCurrentPageAndSetIsLastRecordCommitToTrueIfNecessary();
}

uint32_t WAL::logStructuredNodePropertyPageRecord(
    label_t nodeLabel, uint32_t propertyID, uint32_t pageIdxInOriginalFile) {
    lock_t lck{mtx};
    uint32_t pageIdxInWAL = fileHandle->addNewPage();
    WALRecord walRecord = WALRecord::newStructuredNodePropertyPageUpdateRecord(
        nodeLabel, propertyID, pageIdxInOriginalFile, pageIdxInWAL);
    addNewWALRecordWithoutLock(walRecord);
    return pageIdxInWAL;
}

uint32_t WAL::logStructuredAdjColumnPropertyPage(
    label_t nodeLabel, label_t relLabel, uint32_t propertyID, uint32_t pageIdxInOriginalFile) {
    lock_t lck{mtx};
    uint32_t pageIdxInWAL = fileHandle->addNewPage();
    WALRecord walRecord = WALRecord::newStructuredAdjColumnPropertyPageUpdateRecord(
        nodeLabel, relLabel, propertyID, pageIdxInOriginalFile, pageIdxInWAL);
    addNewWALRecordWithoutLock(walRecord);
    return pageIdxInWAL;
}

void WAL::logCommit(uint64_t transactionID) {
    lock_t lck{mtx};
    WALRecord walRecord = WALRecord::newCommitRecord(transactionID);
    addNewWALRecordWithoutLock(walRecord);
}

void WAL::clearWAL() {
    bufferManager.removeFilePagesFromFrames(*fileHandle);
    fileHandle->resetToZeroPagesAndPageCapacity();
    initCurrentPageAndSetIsLastRecordCommitToTrueIfNecessary();
}

void WAL::flushAllPages(BufferManager* bufferManager) {
    if (!isEmptyWAL()) {
        flushHeaderPages();
        bufferManager->flushAllDirtyPagesInFrames(*fileHandle);
    }
}

void WAL::initCurrentPageAndSetIsLastRecordCommitToTrueIfNecessary() {
    currentHeaderPageIdx = 0;
    if (fileHandle->getNumPages() == 0) {
        fileHandle->addNewPage();
        resetCurrentHeaderPagePrefix();
    } else {
        // If the file existed, read the first page into the currentHeaderPageBuffer.
        fileHandle->readPage(currentHeaderPageBuffer.get(), 0);
        setIsLastRecordCommitToTrueIfNecessary();
    }
}

void WAL::addNewWALRecordWithoutLock(WALRecord& walRecord) {
    if (offsetInCurrentHeaderPage + walRecord.numBytesToWrite() > WAL_HEADER_PAGE_SIZE) {
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
    isLastLoggedRecordCommit_ = (COMMIT_RECORD == walRecord.recordType);
}

void WAL::setIsLastRecordCommitToTrueIfNecessary() {
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
    if (COMMIT_RECORD == walRecord.recordType) {
        isLastLoggedRecordCommit_ = true;
    }
}

WALIterator::WALIterator(shared_ptr<FileHandle> fileHandle, mutex& mtx)
    : BaseWALAndWALIterator(fileHandle), mtx{mtx} {
    resetCurrentHeaderPagePrefix();
    if (fileHandle->getNumPages() > 0) {
        fileHandle->readPage(currentHeaderPageBuffer.get(), 0 /* first header page is at idx 0 */);
    }
    numRecordsReadInCurrentHeaderPage = 0;
}

void WALIterator::getNextRecord(WALRecord& retVal) {
    lock_t lck{mtx};
    if (!hasNextRecordWithoutLock()) {
        throw RuntimeException("WALIterator cannot read  more log records from the WAL.");
    }
    WALRecord::constructWALRecordFromBytes(
        retVal, currentHeaderPageBuffer.get(), offsetInCurrentHeaderPage);
    numRecordsReadInCurrentHeaderPage++;
    if ((numRecordsReadInCurrentHeaderPage == getNumRecordsInCurrentHeaderPage()) &&
        (getNextHeaderPageOfCurrentHeaderPage() != UINT64_MAX)) {
        uint64_t nextHeaderPageIdx = getNextHeaderPageOfCurrentHeaderPage();
        fileHandle->readPage(currentHeaderPageBuffer.get(), nextHeaderPageIdx);
        offsetInCurrentHeaderPage = WAL_HEADER_PAGE_PREFIX_FIELD_SIZES;
        numRecordsReadInCurrentHeaderPage = 0;
    }
}
} // namespace storage
} // namespace graphflow
