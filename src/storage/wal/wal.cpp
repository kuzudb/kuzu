#include "storage/wal/wal.h"

#include "binder/ddl/bound_alter_info.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "common/constants.h"
#include "common/file_system/file_info.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/serializer.h"
#include "common/vector/value_vector.h"
#include "main/db_config.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace storage {

WAL::WAL(const std::string& directory, bool readOnly, VirtualFileSystem* vfs,
    main::ClientContext* context)
    : directory{directory}, vfs{vfs} {
    if (main::DBConfig::isDBPathInMemory(directory)) {
        return;
    }
    fileInfo =
        vfs->openFile(vfs->joinPath(directory, std::string(StorageConstants::WAL_FILE_SUFFIX)),
            FileOpenFlags(readOnly ? FileFlags::READ_ONLY :
                                     FileFlags::CREATE_IF_NOT_EXISTS | FileFlags::READ_ONLY |
                                         FileFlags::WRITE),
            context);
    bufferedWriter = std::make_shared<BufferedFileWriter>(*fileInfo);
    // WAL should always be APPEND only. We don't want to overwrite the file as it may still contain
    // records not replayed. This can happen if checkpoint is not triggered before the Database is
    // closed last time.
    bufferedWriter->setFileOffset(fileInfo->getFileSize());
}

WAL::~WAL() {}

void WAL::logBeginTransaction() {
    std::unique_lock lck{mtx};
    BeginTransactionRecord walRecord;
    addNewWALRecordNoLock(walRecord);
}

void WAL::logAndFlushCommit() {
    std::unique_lock lck{mtx};
    // Flush all pages before committing to make sure that commits only show up in the file when
    // their data is also written.
    CommitRecord walRecord;
    addNewWALRecordNoLock(walRecord);
    flushAllPages();
}

void WAL::logRollback() {
    std::unique_lock lck{mtx};
    RollbackRecord walRecord;
    addNewWALRecordNoLock(walRecord);
}

void WAL::logAndFlushCheckpoint() {
    std::unique_lock lck{mtx};
    CheckpointRecord walRecord;
    addNewWALRecordNoLock(walRecord);
    flushAllPages();
}

void WAL::logCreateCatalogEntryRecord(CatalogEntry* catalogEntry, bool isInternal) {
    std::unique_lock lck{mtx};
    CreateCatalogEntryRecord walRecord(catalogEntry, isInternal);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logCreateCatalogEntryRecord(CatalogEntry* catalogEntry,
    std::vector<CatalogEntry*> childrenEntries, bool isInternal) {
    std::unique_lock lck{mtx};
    CreateCatalogEntryRecord walRecord(catalogEntry, isInternal);
    walRecord.childrenEntries = childrenEntries;
    addNewWALRecordNoLock(walRecord);
}

void WAL::logDropCatalogEntryRecord(table_id_t tableID, CatalogEntryType type) {
    std::unique_lock lck{mtx};
    DropCatalogEntryRecord walRecord(tableID, type);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logAlterCatalogEntryRecord(const BoundAlterInfo* alterInfo) {
    std::unique_lock lck{mtx};
    AlterTableEntryRecord walRecord(alterInfo);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logTableInsertion(table_id_t tableID, TableType tableType, row_idx_t numRows,
    const std::vector<ValueVector*>& vectors) {
    std::unique_lock lck{mtx};
    TableInsertionRecord walRecord(tableID, tableType, numRows, vectors);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logNodeDeletion(table_id_t tableID, offset_t nodeOffset, ValueVector* pkVector) {
    std::unique_lock lck{mtx};
    NodeDeletionRecord walRecord(tableID, nodeOffset, pkVector);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logNodeUpdate(table_id_t tableID, column_id_t columnID, offset_t nodeOffset,
    ValueVector* propertyVector) {
    std::unique_lock lck{mtx};
    NodeUpdateRecord walRecord(tableID, columnID, nodeOffset, propertyVector);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logRelDelete(table_id_t tableID, ValueVector* srcNodeVector, ValueVector* dstNodeVector,
    ValueVector* relIDVector) {
    std::unique_lock lck{mtx};
    RelDeletionRecord walRecord(tableID, srcNodeVector, dstNodeVector, relIDVector);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logRelDetachDelete(table_id_t tableID, RelDataDirection direction,
    ValueVector* srcNodeVector) {
    std::unique_lock lck{mtx};
    RelDetachDeleteRecord walRecord(tableID, direction, srcNodeVector);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logRelUpdate(table_id_t tableID, column_id_t columnID, ValueVector* srcNodeVector,
    ValueVector* dstNodeVector, ValueVector* relIDVector, ValueVector* propertyVector) {
    std::unique_lock lck{mtx};
    RelUpdateRecord walRecord(tableID, columnID, srcNodeVector, dstNodeVector, relIDVector,
        propertyVector);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logCopyTableRecord(table_id_t tableID) {
    std::unique_lock lck{mtx};
    CopyTableRecord walRecord(tableID);
    addToUpdatedTables(tableID);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logUpdateSequenceRecord(sequence_id_t sequenceID, uint64_t kCount) {
    std::unique_lock lck{mtx};
    UpdateSequenceRecord walRecord(sequenceID, kCount);
    addNewWALRecordNoLock(walRecord);
}

void WAL::clearWAL() {
    bufferedWriter->getFileInfo().truncate(0);
    bufferedWriter->resetOffsets();
    updatedTables.clear();
}

// NOLINTNEXTLINE(readability-make-member-function-const): semantically non-const function.
void WAL::flushAllPages() {
    bufferedWriter->flush();
    bufferedWriter->getFileInfo().syncFile();
}

// NOLINTNEXTLINE(readability-make-member-function-const): semantically non-const function.
void WAL::addNewWALRecordNoLock(const WALRecord& walRecord) {
    KU_ASSERT(walRecord.type != WALRecordType::INVALID_RECORD);
    KU_ASSERT(!main::DBConfig::isDBPathInMemory(directory));
    Serializer serializer(bufferedWriter);
    walRecord.serialize(serializer);
}

} // namespace storage
} // namespace kuzu
