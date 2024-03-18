#include "storage/wal_replayer.h"

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/storage.h"
#include "storage/storage_manager.h"
#include "storage/storage_utils.h"
#include "storage/wal_replayer_utils.h"
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
WALReplayer::WALReplayer(WAL* wal, StorageManager* storageManager, BufferManager* bufferManager,
    Catalog* catalog, WALReplayMode replayMode, common::VirtualFileSystem* vfs)
    : isRecovering{replayMode == WALReplayMode::RECOVERY_CHECKPOINT},
      isCheckpoint{replayMode != WALReplayMode::ROLLBACK}, storageManager{storageManager},
      bufferManager{bufferManager}, vfs{vfs}, wal{wal}, catalog{catalog} {
    init();
}

void WALReplayer::init() {
    walFileHandle = wal->fileHandle;
    pageBuffer = std::make_unique<uint8_t[]>(BufferPoolConstants::PAGE_4KB_SIZE);
}

void WALReplayer::replay() {
    // Note: We assume no other thread is accessing the wal during the following operations.
    // If this assumption no longer holds, we need to lock the wal.
    if (!isRecovering && isCheckpoint && !wal->isLastLoggedRecordCommit()) {
        throw StorageException(
            "Cannot checkpointInMemory WAL because last logged record is not a commit record.");
    }
    if (!wal->isEmptyWAL()) {
        auto walIterator = wal->getIterator();
        WALRecord walRecord;
        while (walIterator->hasNextRecord()) {
            walIterator->getNextRecord(walRecord);
            replayWALRecord(walRecord);
        }
    }
    // We next perform an in-memory checkpointing or rolling back of node/relTables.
    if (!wal->getUpdatedTables().empty()) {
        if (isCheckpoint) {
            storageManager->checkpointInMemory();
        } else {
            storageManager->rollbackInMemory();
        }
    }
}

void WALReplayer::replayWALRecord(WALRecord& walRecord) {
    switch (walRecord.recordType) {
    case WALRecordType::PAGE_UPDATE_OR_INSERT_RECORD: {
        replayPageUpdateOrInsertRecord(walRecord);
    } break;
    case WALRecordType::TABLE_STATISTICS_RECORD: {
        replayTableStatisticsRecord(walRecord);
    } break;
    case WALRecordType::COMMIT_RECORD: {
    } break;
    case WALRecordType::CATALOG_RECORD: {
        replayCatalogRecord();
    } break;
    case WALRecordType::CREATE_TABLE_RECORD: {
        replayCreateTableRecord(walRecord);
    } break;
    case WALRecordType::CREATE_RDF_GRAPH_RECORD: {
        replayRdfGraphRecord(walRecord);
    } break;
    case WALRecordType::COPY_TABLE_RECORD: {
        replayCopyTableRecord(walRecord);
    } break;
    case WALRecordType::DROP_TABLE_RECORD: {
        replayDropTableRecord(walRecord);
    } break;
    case WALRecordType::DROP_PROPERTY_RECORD: {
        replayDropPropertyRecord(walRecord);
    } break;
    case WALRecordType::ADD_PROPERTY_RECORD: {
        replayAddPropertyRecord(walRecord);
    } break;
    default:
        throw RuntimeException(
            "Unrecognized WAL record type inside WALReplayer::replay. recordType: " +
            walRecordTypeToString(walRecord.recordType));
    }
}

void WALReplayer::replayPageUpdateOrInsertRecord(const WALRecord& walRecord) {
    // 1. As the first step we copy over the page on disk, regardless of if we are recovering
    // (and checkpointing) or checkpointing while during regular execution.
    auto dbFileID = walRecord.pageInsertOrUpdateRecord.dbFileID;
    std::unique_ptr<FileInfo> fileInfoOfDBFile =
        StorageUtils::getFileInfoForReadWrite(wal->getDirectory(), dbFileID, vfs);
    if (isCheckpoint) {
        if (!wal->isLastLoggedRecordCommit()) {
            // Nothing to undo.
            return;
        }
        walFileHandle->readPage(pageBuffer.get(), walRecord.pageInsertOrUpdateRecord.pageIdxInWAL);
        fileInfoOfDBFile->writeFile(pageBuffer.get(), BufferPoolConstants::PAGE_4KB_SIZE,
            walRecord.pageInsertOrUpdateRecord.pageIdxInOriginalFile *
                BufferPoolConstants::PAGE_4KB_SIZE);
    }
    if (!isRecovering) {
        // 2: If we are not recovering, we do any in-memory checkpointing or rolling back work
        // to make sure that the system's in-memory structures are consistent with what is on
        // disk. For example, we update the BM's image of the pages or InMemDiskArrays used by
        // lists or the WALVersion pageIdxs of pages for VersionedFileHandles.
        checkpointOrRollbackVersionedFileHandleAndBufferManager(walRecord, dbFileID);
    }
}

void WALReplayer::replayTableStatisticsRecord(const WALRecord& walRecord) {
    if (isCheckpoint) {
        if (walRecord.tableStatisticsRecord.isNodeTable) {
            auto walFilePath = StorageUtils::getNodesStatisticsAndDeletedIDsFilePath(
                vfs, wal->getDirectory(), common::FileVersionType::WAL_VERSION);
            auto originalFilePath = StorageUtils::getNodesStatisticsAndDeletedIDsFilePath(
                vfs, wal->getDirectory(), common::FileVersionType::ORIGINAL);
            vfs->overwriteFile(walFilePath, originalFilePath);
            if (!isRecovering) {
                storageManager->getNodesStatisticsAndDeletedIDs()->checkpointInMemoryIfNecessary();
            }
        } else {
            auto walFilePath = StorageUtils::getRelsStatisticsFilePath(
                vfs, wal->getDirectory(), common::FileVersionType::WAL_VERSION);
            auto originalFilePath = StorageUtils::getRelsStatisticsFilePath(
                vfs, wal->getDirectory(), common::FileVersionType::ORIGINAL);
            vfs->overwriteFile(walFilePath, originalFilePath);
            if (!isRecovering) {
                storageManager->getRelsStatistics()->checkpointInMemoryIfNecessary();
            }
        }
    } else {
        if (walRecord.tableStatisticsRecord.isNodeTable) {
            storageManager->getNodesStatisticsAndDeletedIDs()->rollbackInMemoryIfNecessary();
        } else {
            storageManager->getRelsStatistics()->rollbackInMemoryIfNecessary();
        }
    }
}

void WALReplayer::replayCatalogRecord() {
    if (isCheckpoint) {
        auto walFile = StorageUtils::getCatalogFilePath(
            vfs, wal->getDirectory(), common::FileVersionType::WAL_VERSION);
        auto originalFile = StorageUtils::getCatalogFilePath(
            vfs, wal->getDirectory(), common::FileVersionType::ORIGINAL);
        vfs->overwriteFile(walFile, originalFile);
        if (!isRecovering) {
            catalog->checkpointInMemory();
        }
    } else {
        // Since DDL statements are single statements that are auto committed, it is impossible
        // for users to roll back a DDL statement.
    }
}

void WALReplayer::replayCreateTableRecord(const WALRecord& walRecord) {
    if (!isCheckpoint) {
        storageManager->dropTable(walRecord.createTableRecord.tableID);
    }
}

void WALReplayer::replayRdfGraphRecord(const WALRecord& walRecord) {
    WALRecord resourceTableWALRecord = {.recordType = WALRecordType::CREATE_TABLE_RECORD,
        .createTableRecord = walRecord.rdfGraphRecord.resourceTableRecord};
    replayCreateTableRecord(resourceTableWALRecord);
    WALRecord literalTableWALRecord = {.recordType = WALRecordType::CREATE_TABLE_RECORD,
        .createTableRecord = walRecord.rdfGraphRecord.literalTableRecord};
    replayCreateTableRecord(literalTableWALRecord);

    WALRecord resourceTripleTableWALRecord = {.recordType = WALRecordType::CREATE_TABLE_RECORD,
        .createTableRecord = walRecord.rdfGraphRecord.resourceTripleTableRecord};
    replayCreateTableRecord(resourceTripleTableWALRecord);
    WALRecord literalTripleTableWALRecord = {.recordType = WALRecordType::CREATE_TABLE_RECORD,
        .createTableRecord = walRecord.rdfGraphRecord.literalTripleTableRecord};
    replayCreateTableRecord(literalTripleTableWALRecord);
}

void WALReplayer::replayCopyTableRecord(const WALRecord& walRecord) {
    auto tableID = walRecord.copyTableRecord.tableID;
    if (isCheckpoint) {
        if (!isRecovering) {
            // CHECKPOINT.
            // If we are not recovering, i.e., we are checkpointing during normal execution,
            // then we need to update the nodeTable because the actual columns and lists
            // files have been changed during checkpoint. So the in memory
            // fileHandles are obsolete and should be reconstructed (e.g. since the numPages
            // have likely changed they need to reconstruct their page locks).
            auto catalogEntry = catalog->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, tableID);
            if (catalogEntry->getType() == CatalogEntryType::NODE_TABLE_ENTRY) {
                auto nodeTableEntry =
                    ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(catalogEntry);
                storageManager->getNodeTable(tableID)->initializePKIndex(
                    nodeTableEntry, false /* readOnly */, vfs);
            }
        } else {
            // RECOVERY.
            if (wal->isLastLoggedRecordCommit()) {
                return;
            }
            // TODO(Guodong): Do nothing for now. Should remove metaDA and reclaim free pages.
        }
    } else {
        // ROLLBACK.
        // TODO(Guodong): Do nothing for now. Should remove metaDA and reclaim free pages.
    }
}

void WALReplayer::replayDropTableRecord(const WALRecord& walRecord) {
    if (isCheckpoint) {
        auto tableID = walRecord.dropTableRecord.tableID;
        if (!isRecovering) {
            auto tableEntry = catalog->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, tableID);
            switch (tableEntry->getTableType()) {
            case TableType::NODE: {
                storageManager->dropTable(tableID);
                // TODO(Guodong): Do nothing for now. Should remove metaDA and reclaim free pages.
                WALReplayerUtils::removeHashIndexFile(vfs, tableID, wal->getDirectory());
            } break;
            case TableType::REL: {
                storageManager->dropTable(tableID);
                // TODO(Guodong): Do nothing for now. Should remove metaDA and reclaim free pages.
            } break;
            case TableType::RDF: {
                // Do nothing.
            } break;
            default: {
                KU_UNREACHABLE;
            }
            }
        } else {
            if (!wal->isLastLoggedRecordCommit()) {
                // Nothing to undo.
                return;
            }
            auto catalogForRecovery = getCatalogForRecovery(FileVersionType::ORIGINAL);
            auto tableEntry =
                catalogForRecovery->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, tableID);
            switch (tableEntry->getTableType()) {
            case TableType::NODE: {
                // TODO(Guodong): Do nothing for now. Should remove metaDA and reclaim free pages.
                WALReplayerUtils::removeHashIndexFile(vfs, tableID, wal->getDirectory());
            } break;
            case TableType::REL:
            case TableType::RDF: {
                // TODO(Guodong): Do nothing for now. Should remove metaDA and reclaim free pages.
            } break;
            default: {
                KU_UNREACHABLE;
            }
            }
        }
    } else {
        // See comments for COPY_NODE_RECORD.
    }
}

void WALReplayer::replayDropPropertyRecord(const WALRecord& walRecord) {
    if (isCheckpoint) {
        auto tableID = walRecord.dropPropertyRecord.tableID;
        auto propertyID = walRecord.dropPropertyRecord.propertyID;
        if (!isRecovering) {
            auto tableEntry = catalog->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, tableID);
            switch (tableEntry->getTableType()) {
            case TableType::NODE: {
                storageManager->getNodeTable(tableID)->dropColumn(
                    tableEntry->getColumnID(propertyID));
                // TODO(Guodong): Do nothing for now. Should remove metaDA and reclaim free pages.
            } break;
            case TableType::REL: {
                storageManager->getRelTable(tableID)->dropColumn(
                    tableEntry->getColumnID(propertyID));
                // TODO(Guodong): Do nothing for now. Should remove metaDA and reclaim free pages.
            } break;
            default: {
                KU_UNREACHABLE;
            }
            }
        } else {
            if (!wal->isLastLoggedRecordCommit()) {
                // Nothing to undo.
                return;
            }
            // TODO(Guodong): Do nothing for now. Should remove metaDA and reclaim free pages.
        }
    } else {
        // See comments for COPY_NODE_RECORD.
    }
}

void WALReplayer::replayAddPropertyRecord(const WALRecord& walRecord) {
    auto tableID = walRecord.addPropertyRecord.tableID;
    auto propertyID = walRecord.addPropertyRecord.propertyID;
    if (!isCheckpoint) {
        auto tableEntry = catalog->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, tableID);
        switch (tableEntry->getTableType()) {
        case TableType::NODE: {
            storageManager->getNodeTable(tableID)->dropColumn(tableEntry->getColumnID(propertyID));
        } break;
        case TableType::REL: {
            storageManager->getRelTable(tableID)->dropColumn(tableEntry->getColumnID(propertyID));
        } break;
        default: {
            KU_UNREACHABLE;
        }
        }
    }
}

void WALReplayer::truncateFileIfInsertion(
    BMFileHandle* fileHandle, const PageUpdateOrInsertRecord& pageInsertOrUpdateRecord) {
    if (pageInsertOrUpdateRecord.isInsert) {
        // If we are rolling back and this is a page insertion we truncate the fileHandle's
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
    if (fileHandle) {
        fileHandle->clearWALPageIdxIfNecessary(
            walRecord.pageInsertOrUpdateRecord.pageIdxInOriginalFile);
        if (isCheckpoint) {
            // Update the page in buffer manager if it is in a frame. Note that we assume
            // that the pageBuffer currently contains the contents of the WALVersion, so the
            // caller needs to make sure that this assumption holds.
            bufferManager->updateFrameIfPageIsInFrameWithoutLock(*fileHandle, pageBuffer.get(),
                walRecord.pageInsertOrUpdateRecord.pageIdxInOriginalFile);
        } else {
            truncateFileIfInsertion(fileHandle, walRecord.pageInsertOrUpdateRecord);
        }
    }
}

BMFileHandle* WALReplayer::getVersionedFileHandleIfWALVersionAndBMShouldBeCleared(
    const DBFileID& dbFileID) {
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

std::unique_ptr<Catalog> WALReplayer::getCatalogForRecovery(FileVersionType fileVersionType) {
    // When we are recovering our database, the catalog field of walReplayer has not been
    // initialized and recovered yet. We need to create a new catalog to get node/rel tableEntries
    // for recovering.
    auto catalogForRecovery = std::make_unique<Catalog>(vfs);
    catalogForRecovery->getReadOnlyVersion()->readFromFile(wal->getDirectory(), fileVersionType);
    return catalogForRecovery;
}

} // namespace storage
} // namespace kuzu
