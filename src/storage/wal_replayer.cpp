#include "storage/wal_replayer.h"

#include <unordered_map>

#include "catalog/catalog_entry/scalar_macro_catalog_entry.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/file_system/file_info.h"
#include "common/serializer/buffered_file.h"
#include "storage/storage_manager.h"
#include "storage/storage_utils.h"
#include "storage/wal/wal_record.h"
#include "transaction/transaction.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

// COMMIT_CHECKPOINT:   isCheckpoint = true,  isRecovering = false
// ROLLBACK:            isCheckpoint = false, isRecovering = false
// RECOVERY_CHECKPOINT: isCheckpoint = true,  isRecovering = true
WALReplayer::WALReplayer(main::ClientContext& clientContext, BMFileHandle& shadowFH,
    WALReplayMode replayMode)
    : shadowFH{shadowFH}, isRecovering{replayMode == WALReplayMode::RECOVERY_CHECKPOINT},
      isCheckpoint{replayMode != WALReplayMode::ROLLBACK}, clientContext{clientContext} {
    walFilePath = clientContext.getVFSUnsafe()->joinPath(clientContext.getDatabasePath(),
        StorageConstants::WAL_FILE_SUFFIX);
    pageBuffer = std::make_unique<uint8_t[]>(BufferPoolConstants::PAGE_4KB_SIZE);
}

void WALReplayer::replay() {
    if (!clientContext.getVFSUnsafe()->fileOrPathExists(walFilePath)) {
        return;
    }
    auto fileInfo = clientContext.getVFSUnsafe()->openFile(walFilePath, O_RDONLY);
    auto walFileSize = fileInfo->getFileSize();
    // Check if the wal file is empty or corrupted. so nothing to read.
    if (walFileSize == 0) {
        return;
    }
    // TODO(Guodong): Handle the case that wal file is corrupted and there is no COMMIT record for
    // the last transaction.
    try {
        Deserializer deserializer(std::make_unique<BufferedFileReader>(std::move(fileInfo)));
        std::unordered_map<DBFileID, std::unique_ptr<FileInfo>> fileCache;
        while (!deserializer.finished()) {
            auto walRecord = WALRecord::deserialize(deserializer);
            replayWALRecord(*walRecord, fileCache);
        }
    } catch (const Exception& e) {
        throw RuntimeException(
            stringFormat("Failed to read wal record from WAL file. Error: {}", e.what()));
    }
}

void WALReplayer::replayWALRecord(WALRecord& walRecord,
    std::unordered_map<DBFileID, std::unique_ptr<FileInfo>>& fileCache) {
    switch (walRecord.type) {
    case WALRecordType::PAGE_UPDATE_OR_INSERT_RECORD: {
        replayPageUpdateOrInsertRecord(walRecord, fileCache);
    } break;
    case WALRecordType::CATALOG_RECORD: {
        replayCatalogRecord(walRecord);
    } break;
    case WALRecordType::TABLE_STATISTICS_RECORD: {
        replayTableStatisticsRecord(walRecord);
    } break;
    case WALRecordType::COMMIT_RECORD: {
    } break;
    case WALRecordType::CREATE_CATALOG_ENTRY_RECORD: {
        replayCreateCatalogEntryRecord(walRecord);
    } break;
    case WALRecordType::COPY_TABLE_RECORD: {
        replayCopyTableRecord(walRecord);
    } break;
    case WALRecordType::DROP_CATALOG_ENTRY_RECORD: {
        replayDropCatalogEntryRecord(walRecord);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void WALReplayer::replayPageUpdateOrInsertRecord(const WALRecord& walRecord,
    std::unordered_map<DBFileID, std::unique_ptr<FileInfo>>& fileCache) {
    // 1. As the first step we copy over the page on disk, regardless of if we are recovering
    // (and checkpointing) or checkpointing while during regular execution.
    auto& pageInsertOrUpdateRecord =
        ku_dynamic_cast<const WALRecord&, const PageUpdateOrInsertRecord&>(walRecord);
    auto dbFileID = pageInsertOrUpdateRecord.dbFileID;
    auto entry = fileCache.find(dbFileID);
    if (entry == fileCache.end()) {
        fileCache.insert(std::make_pair(dbFileID,
            StorageUtils::getFileInfoForReadWrite(clientContext.getDatabasePath(), dbFileID,
                clientContext.getVFSUnsafe())));
        entry = fileCache.find(dbFileID);
    }
    auto& fileInfoOfDBFile = entry->second;
    if (isCheckpoint) {
        shadowFH.readPage(pageBuffer.get(), pageInsertOrUpdateRecord.pageIdxInWAL);
        fileInfoOfDBFile->writeFile(pageBuffer.get(), BufferPoolConstants::PAGE_4KB_SIZE,
            pageInsertOrUpdateRecord.pageIdxInOriginalFile * BufferPoolConstants::PAGE_4KB_SIZE);
    }
    if (!isRecovering) {
        // 2: If we are not recovering, we do any in-memory checkpointing or rolling back work
        // to make sure that the system's in-memory structures are consistent with what is on
        // disk. For example, we update the BM's image of the pages or InMemDiskArrays used by
        // lists or the WALVersion pageIdxs of pages for VersionedFileHandles.
        checkpointOrRollbackVersionedFileHandleAndBufferManager(walRecord, dbFileID);
    }
}

void WALReplayer::replayCatalogRecord(const WALRecord&) {
    if (isCheckpoint) {
        auto vfs = clientContext.getVFSUnsafe();
        auto checkpointFile = StorageUtils::getCatalogFilePath(vfs, clientContext.getDatabasePath(),
            common::FileVersionType::WAL_VERSION);
        auto originalFile = StorageUtils::getCatalogFilePath(vfs, clientContext.getDatabasePath(),
            common::FileVersionType::ORIGINAL);
        vfs->overwriteFile(checkpointFile, originalFile);
    }
}

void WALReplayer::replayTableStatisticsRecord(const WALRecord& walRecord) {
    auto& tableStatisticsRecord =
        ku_dynamic_cast<const WALRecord&, const TableStatisticsRecord&>(walRecord);
    auto vfs = clientContext.getVFSUnsafe();
    auto storageManager = clientContext.getStorageManager();
    if (isCheckpoint) {
        switch (tableStatisticsRecord.tableType) {
        case TableType::NODE: {
            auto checkpointFile = StorageUtils::getNodesStatisticsAndDeletedIDsFilePath(vfs,
                clientContext.getDatabasePath(), common::FileVersionType::WAL_VERSION);
            if (!vfs->fileOrPathExists(walFilePath)) {
                // This is a temp hack: multiple transactions can log multiple table stats records
                // before checkpoint.
                return;
            }
            auto originalFilePath = StorageUtils::getNodesStatisticsAndDeletedIDsFilePath(vfs,
                clientContext.getDatabasePath(), common::FileVersionType::ORIGINAL);
            vfs->overwriteFile(checkpointFile, originalFilePath);
            if (!isRecovering) {
                storageManager->getNodesStatisticsAndDeletedIDs()->checkpointInMemoryIfNecessary();
            }
        } break;
        case TableType::REL: {
            auto checkpointFile = StorageUtils::getRelsStatisticsFilePath(vfs,
                clientContext.getDatabasePath(), common::FileVersionType::WAL_VERSION);
            if (!vfs->fileOrPathExists(checkpointFile)) {
                // This is a temp hack: multiple transactions can log multiple table stats records
                // before checkpoint.
                return;
            }
            auto originalFilePath = StorageUtils::getRelsStatisticsFilePath(vfs,
                clientContext.getDatabasePath(), common::FileVersionType::ORIGINAL);
            vfs->overwriteFile(checkpointFile, originalFilePath);
            if (!isRecovering) {
                storageManager->getRelsStatistics()->checkpointInMemoryIfNecessary();
            }
        } break;
        default: {
            KU_UNREACHABLE;
        }
        }
    } else {
        switch (tableStatisticsRecord.tableType) {
        case TableType::NODE: {
            storageManager->getNodesStatisticsAndDeletedIDs()->rollbackInMemoryIfNecessary();
        } break;
        case TableType::REL: {
            storageManager->getRelsStatistics()->rollbackInMemoryIfNecessary();
        } break;
        default: {
            KU_UNREACHABLE;
        }
        }
    }
}

void WALReplayer::replayCreateCatalogEntryRecord(const WALRecord& walRecord) {
    if (!(isCheckpoint && isRecovering)) {
        // Nothing to do.
        return;
    }
    auto& createEntryRecord =
        ku_dynamic_cast<const WALRecord&, const CreateCatalogEntryRecord&>(walRecord);
    switch (createEntryRecord.ownedCatalogEntry->getType()) {
    case CatalogEntryType::NODE_TABLE_ENTRY:
    case CatalogEntryType::REL_TABLE_ENTRY:
    case CatalogEntryType::REL_GROUP_ENTRY:
    case CatalogEntryType::RDF_GRAPH_ENTRY: {
        auto tableEntry = ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(
            createEntryRecord.ownedCatalogEntry.get());
        clientContext.getCatalog()->createTableSchema(&DUMMY_WRITE_TRANSACTION,
            tableEntry->getBoundCreateTableInfo(&DUMMY_WRITE_TRANSACTION));
    } break;
    case CatalogEntryType::SCALAR_MACRO_ENTRY: {
        auto macroEntry = ku_dynamic_cast<CatalogEntry*, ScalarMacroCatalogEntry*>(
            createEntryRecord.ownedCatalogEntry.get());
        clientContext.getCatalog()->addScalarMacroFunction(&DUMMY_WRITE_TRANSACTION,
            macroEntry->getName(), macroEntry->getMacroFunction()->copy());
    } break;
    case CatalogEntryType::SEQUENCE_ENTRY: {
        auto sequenceEntry = ku_dynamic_cast<CatalogEntry*, SequenceCatalogEntry*>(
            createEntryRecord.ownedCatalogEntry.get());
        clientContext.getCatalog()->createSequence(&DUMMY_WRITE_TRANSACTION,
            sequenceEntry->getBoundCreateSequenceInfo());
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

// Replay catalog should only work under RECOVERY mode.
void WALReplayer::replayDropCatalogEntryRecord(const WALRecord& walRecord) {
    if (!(isCheckpoint && isRecovering)) {
        return;
    }
    auto dropEntryRecord =
        ku_dynamic_cast<const WALRecord&, const DropCatalogEntryRecord&>(walRecord);
    auto entryID = dropEntryRecord.entryID;
    switch (dropEntryRecord.entryType) {
    case CatalogEntryType::NODE_TABLE_ENTRY:
    case CatalogEntryType::REL_TABLE_ENTRY:
    case CatalogEntryType::REL_GROUP_ENTRY:
    case CatalogEntryType::RDF_GRAPH_ENTRY: {
        clientContext.getCatalog()->dropTableSchema(&DUMMY_WRITE_TRANSACTION, entryID);
        // During recovery, storageManager does not exist.
        if (clientContext.getStorageManager()) {
            clientContext.getStorageManager()->dropTable(entryID);
        }
    } break;
    case CatalogEntryType::SEQUENCE_ENTRY: {
        clientContext.getCatalog()->dropSequence(&DUMMY_WRITE_TRANSACTION, entryID);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void WALReplayer::replayCopyTableRecord(const WALRecord&) const {
    // DO NOTHING.
    // TODO(Guodong): Should handle metaDA and reclaim free pages when rollback.
}

void WALReplayer::truncateFileIfInsertion(BMFileHandle* fileHandle,
    const PageUpdateOrInsertRecord& pageInsertOrUpdateRecord) {
    if (pageInsertOrUpdateRecord.isInsert) {
        // If we are rolling back and this is a page insertion we truncate the shadowingFH's
        // data structures that hold locks for pageIdxs.
        // Note: We can directly call removePageIdxAndTruncateIfNecessary here because we
        // assume there is a single write transaction in the system at any point in time.
        // Suppose page 5 and page 6 were added to a file during a transaction, which is now
        // rolling back. As we replay the log to rollback, we see page 5's insertion first.
        // However, we can directly truncate the file to 5 (even if later we will see a page
        // 6 insertion), because we assume that if there were further new page additions to
        // the same file, they must also be part of the rolling back transaction. If this
        // assumption fails, instead of truncating, we need to indicate that page 5 is a
        // "free" page again and have some logic to maintain free pages.
        fileHandle->removePageIdxAndTruncateIfNecessary(
            pageInsertOrUpdateRecord.pageIdxInOriginalFile);
    }
}

void WALReplayer::checkpointOrRollbackVersionedFileHandleAndBufferManager(
    const WALRecord& walRecord, const DBFileID& dbFileID) {
    BMFileHandle* fileHandle = getVersionedFileHandleIfWALVersionAndBMShouldBeCleared(dbFileID);
    auto& pageInsertOrUpdateRecord =
        ku_dynamic_cast<const WALRecord&, const PageUpdateOrInsertRecord&>(walRecord);
    if (fileHandle) {
        fileHandle->clearWALPageIdxIfNecessary(pageInsertOrUpdateRecord.pageIdxInOriginalFile);
        if (isCheckpoint) {
            // Update the page in buffer manager if it is in a frame. Note that we assume
            // that the pageBuffer currently contains the contents of the WALVersion, so the
            // caller needs to make sure that this assumption holds.
            clientContext.getMemoryManager()
                ->getBufferManager()
                ->updateFrameIfPageIsInFrameWithoutLock(*fileHandle, pageBuffer.get(),
                    pageInsertOrUpdateRecord.pageIdxInOriginalFile);
        } else {
            truncateFileIfInsertion(fileHandle, pageInsertOrUpdateRecord);
        }
    }
}

BMFileHandle* WALReplayer::getVersionedFileHandleIfWALVersionAndBMShouldBeCleared(
    const DBFileID& dbFileID) {
    auto storageManager = clientContext.getStorageManager();
    switch (dbFileID.dbFileType) {
    case DBFileType::METADATA: {
        return storageManager->getMetadataFH();
    }
    case DBFileType::DATA: {
        return storageManager->getDataFH();
    }
    case DBFileType::NODE_INDEX: {
        auto index = storageManager->getPKIndex(dbFileID.nodeIndexID.tableID);
        return dbFileID.isOverflow ? index->getOverflowFile()->getBMFileHandle() :
                                     index->getFileHandle();
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

} // namespace storage
} // namespace kuzu
