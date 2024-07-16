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
    fileInfo =
        vfs->openFile(vfs->joinPath(directory, std::string(StorageConstants::WAL_FILE_SUFFIX)),
            readOnly ? O_RDONLY : O_CREAT | O_RDWR, context);
    bufferedWriter = std::make_shared<BufferedFileWriter>(*fileInfo);
}

WAL::~WAL() {}

void WAL::logAndFlushCommit(uint64_t transactionID) {
    lock_t lck{mtx};
    // Flush all pages before committing to make sure that commits only show up in the file when
    // their data is also written.
    flushAllPages();
    CommitRecord walRecord(transactionID);
    addNewWALRecordNoLock(walRecord);
}

void WAL::logAndFlushCheckpoint() {
    lock_t lck{mtx};
    CheckpointRecord walRecord;
    addNewWALRecordNoLock(walRecord);
    flushAllPages();
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
