#include "storage/wal/wal.h"

#include <fcntl.h>

#include "binder/ddl/bound_alter_info.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "common/file_system/file_info.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/serializer.h"
#include "storage/storage_utils.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace storage {

WAL::WAL(const std::string& directory, bool readOnly, BufferManager& bufferManager,
    VirtualFileSystem* vfs, main::ClientContext* context)
    : directory{directory}, bufferManager{bufferManager}, vfs{vfs} {
    auto fileInfo =
        vfs->openFile(vfs->joinPath(directory, std::string(StorageConstants::WAL_FILE_SUFFIX)),
            readOnly ? O_RDONLY : O_CREAT | O_RDWR, context);
    bufferedWriter = std::make_shared<BufferedFileWriter>(std::move(fileInfo));
    shadowingFH = bufferManager.getBMFileHandle(
        vfs->joinPath(directory, std::string(StorageConstants::SHADOWING_SUFFIX)),
        readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                   FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::NON_VERSIONED_FILE, vfs, context);
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

void WAL::logDropCatalogEntryRecord(uint64_t tableID, catalog::CatalogEntryType type) {
    KU_ASSERT(
        type == CatalogEntryType::NODE_TABLE_ENTRY || type == CatalogEntryType::REL_TABLE_ENTRY ||
        type == CatalogEntryType::REL_GROUP_ENTRY || type == CatalogEntryType::RDF_GRAPH_ENTRY ||
        type == CatalogEntryType::SEQUENCE_ENTRY);
    lock_t lck{mtx};
    DropCatalogEntryRecord walRecord(tableID, type);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logAlterTableEntryRecord(BoundAlterInfo* alterInfo) {
    lock_t lck{mtx};
    AlterTableEntryRecord walRecord(alterInfo);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logCopyTableRecord(table_id_t tableID) {
    lock_t lck{mtx};
    CopyTableRecord walRecord(tableID);
    addToUpdatedTables(tableID);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logUpdateSequenceRecord(sequence_id_t sequenceID, SequenceChangeData data) {
    lock_t lck{mtx};
    UpdateSequenceRecord walRecord(sequenceID, std::move(data));
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
