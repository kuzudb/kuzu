#include "storage/wal/wal.h"

#include <fcntl.h>

#include "common/file_system/file_info.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/serializer.h"
#include "storage/storage_utils.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

WAL::WAL(const std::string& directory, bool readOnly, BufferManager& bufferManager,
    VirtualFileSystem* vfs)
    : directory{directory}, bufferManager{bufferManager}, vfs{vfs} {
    auto fileInfo =
        vfs->openFile(vfs->joinPath(directory, std::string(StorageConstants::WAL_FILE_SUFFIX)),
            readOnly ? O_RDONLY : O_CREAT | O_RDWR);
    bufferedWriter = std::make_shared<BufferedFileWriter>(std::move(fileInfo));
    shadowingFH = bufferManager.getBMFileHandle(
        vfs->joinPath(directory, std::string(StorageConstants::SHADOWING_SUFFIX)),
        readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                   FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::NON_VERSIONED_FILE, vfs);
}

WAL::~WAL() {}

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

void WAL::logCatalogRecord() {
    lock_t lck{mtx};
    CatalogRecord walRecord;
    addNewWALRecordNoLock(walRecord);
}

void WAL::logTableStatisticsRecord(TableType tableType) {
    lock_t lck{mtx};
    TableStatisticsRecord walRecord(tableType);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logCreateCatalogEntryRecord(CatalogEntry* catalogEntry) {
    lock_t lck{mtx};
    CreateCatalogEntryRecord walRecord(catalogEntry);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logDropTableRecord(table_id_t tableID, catalog::CatalogEntryType type) {
    lock_t lck{mtx};
    DropCatalogEntryRecord walRecord(tableID, type);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logCopyTableRecord(table_id_t tableID) {
    lock_t lck{mtx};
    CopyTableRecord walRecord(tableID);
    addToUpdatedTables(tableID);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logCreateSequenceRecord(catalog::CatalogEntry* catalogEntry) {
    lock_t lck{mtx};
    CreateCatalogEntryRecord walRecord(catalogEntry);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logDropSequenceRecord(sequence_id_t sequenceID) {
    lock_t lck{mtx};
    DropCatalogEntryRecord walRecord(sequenceID, catalog::CatalogEntryType::SEQUENCE_ENTRY);
    addNewWALRecordNoLock(walRecord);
}

void WAL::clearWAL() {
    bufferManager.removeFilePagesFromFrames(*shadowingFH);
    shadowingFH->resetToZeroPagesAndPageCapacity();
    bufferedWriter->getFileInfo().truncate(0);
    bufferedWriter->resetOffsets();
    StorageUtils::removeCatalogAndStatsWALFiles(directory, vfs);
    updatedTables.clear();
}

void WAL::flushAllPages() {
    bufferedWriter->flush();
    bufferManager.flushAllDirtyPagesInFrames(*shadowingFH);
    bufferedWriter->getFileInfo().syncFile();
}

void WAL::addNewWALRecordNoLock(WALRecord& walRecord) {
    KU_ASSERT(walRecord.type != WALRecordType::INVALID_RECORD);
    Serializer serializer(bufferedWriter);
    walRecord.serialize(serializer);
}

} // namespace storage
} // namespace kuzu
