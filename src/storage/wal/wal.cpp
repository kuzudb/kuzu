#include "storage/wal/wal.h"

#include <fcntl.h>

#include "common/exception/io.h"
#include "common/file_system/file_info.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "spdlog/spdlog.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

WAL::WAL(const std::string& directory, bool readOnly, BufferManager& bufferManager,
    VirtualFileSystem* vfs)
    : directory{directory}, bufferManager{bufferManager}, vfs{vfs}, isEmpty{true},
      isLastRecordCommit{false} {
    auto fileInfo =
        vfs->openFile(vfs->joinPath(directory, std::string(StorageConstants::WAL_FILE_SUFFIX)),
            readOnly ? O_RDONLY : O_CREAT | O_RDWR);
    bufferedWriter = std::make_shared<BufferedFileWriter>(std::move(fileInfo));
    shadowingFH = bufferManager.getBMFileHandle(
        vfs->joinPath(directory, std::string(StorageConstants::SHADOWING_SUFFIX)),
        readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                   FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::NON_VERSIONED_FILE, vfs);
    initialize();
}

WAL::~WAL() {
    try {
        flushAllPages();
    } catch (IOException& e) {
        spdlog::error("Failed to flush WAL: {}", e.what());
    }
}

page_idx_t WAL::logPageUpdateRecord(DBFileID dbFileID, page_idx_t pageIdxInOriginalFile) {
    lock_t lck{mtx};
    auto pageIdxInWAL = shadowingFH->addNewPage();
    PageUpdateOrInsertRecord walRecord(dbFileID, pageIdxInOriginalFile, pageIdxInWAL,
        false /*isInsert*/);
    addNewWALRecordNoLock(walRecord);
    return pageIdxInWAL;
}

page_idx_t WAL::logPageInsertRecord(DBFileID dbFileID, page_idx_t pageIdxInOriginalFile) {
    lock_t lck{mtx};
    auto pageIdxInWAL = shadowingFH->addNewPage();
    PageUpdateOrInsertRecord walRecord(dbFileID, pageIdxInOriginalFile, pageIdxInWAL,
        true /*isInsert*/);
    addNewWALRecordNoLock(walRecord);
    return pageIdxInWAL;
}

void WAL::logCommit(uint64_t transactionID) {
    lock_t lck{mtx};
    // Flush all pages before committing to make sure that commits only show up in the file when
    // their data is also written.
    flushAllPages();
    CommitRecord walRecord(transactionID);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logTableStatisticsRecord(TableType tableType) {
    lock_t lck{mtx};
    TableStatisticsRecord walRecord(tableType);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logCatalogRecord() {
    lock_t lck{mtx};
    CatalogRecord walRecord;
    addNewWALRecordNoLock(walRecord);
}

void WAL::logCreateTableRecord(table_id_t tableID, TableType tableType) {
    lock_t lck{mtx};
    KU_ASSERT(tableType == TableType::NODE || tableType == TableType::REL);
    CreateTableRecord walRecord(tableID, tableType);
    addToUpdatedTables(tableID);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logCreateRdfGraphRecord(table_id_t rdfGraphID, table_id_t resourceTableID,
    table_id_t literalTableID, table_id_t resourceTripleTableID, table_id_t literalTripleTableID) {
    lock_t lck{mtx};
    auto resourceTableRecord =
        std::make_unique<CreateTableRecord>(resourceTableID, TableType::NODE);
    auto literalTableRecord = std::make_unique<CreateTableRecord>(literalTableID, TableType::NODE);
    auto resourceTripleTableRecord =
        std::make_unique<CreateTableRecord>(resourceTripleTableID, TableType::REL);
    auto literalTripleTableRecord =
        std::make_unique<CreateTableRecord>(literalTripleTableID, TableType::REL);
    RdfGraphRecord walRecord(rdfGraphID, std::move(resourceTableRecord),
        std::move(literalTableRecord), std::move(resourceTripleTableRecord),
        std::move(literalTripleTableRecord));
    addNewWALRecordNoLock(walRecord);
}

void WAL::logCopyTableRecord(table_id_t tableID) {
    lock_t lck{mtx};
    CopyTableRecord walRecord(tableID);
    addToUpdatedTables(tableID);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logDropTableRecord(table_id_t tableID) {
    lock_t lck{mtx};
    DropTableRecord walRecord(tableID);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logDropPropertyRecord(table_id_t tableID, property_id_t propertyID) {
    lock_t lck{mtx};
    DropPropertyRecord walRecord(tableID, propertyID);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logAddPropertyRecord(table_id_t tableID, property_id_t propertyID) {
    lock_t lck{mtx};
    AddPropertyRecord walRecord(tableID, propertyID);
    addNewWALRecordNoLock(walRecord);
}

void WAL::clearWAL() {
    bufferManager.removeFilePagesFromFrames(*shadowingFH);
    shadowingFH->resetToZeroPagesAndPageCapacity();
    bufferedWriter->getFileInfo().truncate(0);
    bufferedWriter->resetOffsets();
    isEmpty = true;
    isLastRecordCommit = false;
    StorageUtils::removeAllWALFiles(directory);
    updatedTables.clear();
}

void WAL::flushAllPages() {
    bufferedWriter->flush();
    bufferManager.flushAllDirtyPagesInFrames(*shadowingFH);
    bufferedWriter->getFileInfo().syncFile();
}

bool WAL::isEmptyWAL() const {
    return isEmpty;
}

void WAL::initialize() {
    auto& fileInfo = bufferedWriter->getFileInfo();
    auto walFileLength = fileInfo.getFileSize();
    if (walFileLength == 0) {
        // Empty file. Nothing to check.
        isEmpty = true;
        return;
    }
    // Read the last record to check if it is a COMMIT_RECORD.
    std::unique_ptr<WALRecord> walRecord;
    try {
        Deserializer deserializer(std::make_unique<BufferedFileReader>(vfs->openFile(
            vfs->joinPath(directory, std::string(StorageConstants::WAL_FILE_SUFFIX)), O_RDONLY)));
        while (!deserializer.finished()) {
            walRecord = WALRecord::deserialize(deserializer);
        }
    } catch (const Exception& e) {
        throw RuntimeException(
            stringFormat("Failed to read the last record from WAL file. Error: {}", e.what()));
    }
    if (walRecord->type != WALRecordType::COMMIT_RECORD) {
        // The last record is not a COMMIT record. Nothing to replay.
        isLastRecordCommit = false;
        return;
    }
    isLastRecordCommit = true;
    isEmpty = false;
}

void WAL::addNewWALRecordNoLock(WALRecord& walRecord) {
    isEmpty = false;
    Serializer serializer(bufferedWriter);
    walRecord.serialize(serializer);
    isLastRecordCommit = walRecord.type == WALRecordType::COMMIT_RECORD;
}

} // namespace storage
} // namespace kuzu
