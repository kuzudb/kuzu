#include "storage/wal/wal.h"

#include <fcntl.h>

#include "binder/ddl/bound_alter_info.h"
#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "common/file_system/file_info.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/serializer.h"
#include "common/vector/value_vector.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace storage {

WAL::WAL(const std::string& directory, bool readOnly, BufferManager& bufferManager,
    VirtualFileSystem* vfs, main::ClientContext* context)
    : directory{directory}, bufferManager{bufferManager}, vfs{vfs} {
    fileInfo =
        vfs->openFile(vfs->joinPath(directory, std::string(StorageConstants::WAL_FILE_SUFFIX)),
            readOnly ? O_RDONLY : O_CREAT | O_RDWR, context);
    bufferedWriter = std::make_shared<BufferedFileWriter>(*fileInfo);
    // WAL should always be APPEND only. We don't want to overwrite the file as it may still contain
    // records not replayed. This can happen if checkpoint is not triggered before the Database is
    // closed last time.
    bufferedWriter->setFileOffset(fileInfo->getFileSize());
}

WAL::~WAL() {}

void WAL::logBeginTransaction() {
    lock_t lck{mtx};
    BeginTransactionRecord walRecord;
    addNewWALRecordNoLock(walRecord);
}

void WAL::logAndFlushCommit(uint64_t transactionID) {
    lock_t lck{mtx};
    // Flush all pages before committing to make sure that commits only show up in the file when
    // their data is also written.
    CommitRecord walRecord(transactionID);
    addNewWALRecordNoLock(walRecord);
    flushAllPages();
}

void WAL::logAndFlushCheckpoint() {
    lock_t lck{mtx};
    CheckpointRecord walRecord;
    addNewWALRecordNoLock(walRecord);
    flushAllPages();
}

void WAL::logCreateTableEntryRecord(BoundCreateTableInfo tableInfo) {
    lock_t lck{mtx};
    CreateTableEntryRecord walRecord(std::move(tableInfo));
    addNewWALRecordNoLock(walRecord);
}

void WAL::logCreateCatalogEntryRecord(CatalogEntry* catalogEntry) {
    lock_t lck{mtx};
    CreateCatalogEntryRecord walRecord(catalogEntry);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logDropCatalogEntryRecord(table_id_t tableID, CatalogEntryType type) {
    lock_t lck{mtx};
    DropCatalogEntryRecord walRecord(tableID, type);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logAlterTableEntryRecord(const BoundAlterInfo* alterInfo) {
    lock_t lck{mtx};
    AlterTableEntryRecord walRecord(alterInfo);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logTableInsertion(table_id_t tableID, TableType tableType, row_idx_t numRows,
    const std::vector<ValueVector*>& vectors) {
    lock_t lck{mtx};
    TableInsertionRecord walRecord(tableID, tableType, numRows, vectors);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logNodeDeletion(table_id_t tableID, offset_t nodeOffset, ValueVector* pkVector) {
    lock_t lck{mtx};
    NodeDeletionRecord walRecord(tableID, nodeOffset, pkVector);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logNodeUpdate(table_id_t tableID, column_id_t columnID, offset_t nodeOffset,
    ValueVector* propertyVector) {
    lock_t lck{mtx};
    NodeUpdateRecord walRecord(tableID, columnID, nodeOffset, propertyVector);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logRelDelete(table_id_t tableID, ValueVector* srcNodeVector, ValueVector* dstNodeVector,
    ValueVector* relIDVector) {
    lock_t lck{mtx};
    RelDeletionRecord walRecord(tableID, srcNodeVector, dstNodeVector, relIDVector);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logRelDetachDelete(table_id_t tableID, RelDataDirection direction,
    ValueVector* srcNodeVector) {
    lock_t lck{mtx};
    RelDetachDeleteRecord walRecord(tableID, direction, srcNodeVector);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logRelUpdate(table_id_t tableID, column_id_t columnID, ValueVector* srcNodeVector,
    ValueVector* dstNodeVector, ValueVector* relIDVector, ValueVector* propertyVector) {
    lock_t lck{mtx};
    RelUpdateRecord walRecord(tableID, columnID, srcNodeVector, dstNodeVector, relIDVector,
        propertyVector);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logCopyTableRecord(table_id_t tableID) {
    lock_t lck{mtx};
    CopyTableRecord walRecord(tableID);
    addToUpdatedTables(tableID);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logUpdateSequenceRecord(sequence_id_t sequenceID, uint64_t kCount) {
    lock_t lck{mtx};
    UpdateSequenceRecord walRecord(sequenceID, kCount);
    addNewWALRecordNoLock(walRecord);
}

void WAL::clearWAL() {
    bufferedWriter->getFileInfo().truncate(0);
    bufferedWriter->resetOffsets();
    updatedTables.clear();
}

void WAL::flushAllPages() {
    bufferedWriter->flush();
    bufferedWriter->getFileInfo().syncFile();
}

void WAL::addNewWALRecordNoLock(const WALRecord& walRecord) {
    KU_ASSERT(walRecord.type != WALRecordType::INVALID_RECORD);
    Serializer serializer(bufferedWriter);
    walRecord.serialize(serializer);
}

} // namespace storage
} // namespace kuzu
