#include "storage/wal_replayer.h"

#include "catalog/node_table_schema.h"
#include "common/exception/storage.h"
#include "storage/storage_manager.h"
#include "storage/storage_utils.h"
#include "storage/wal_replayer_utils.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace storage {

// COMMIT_CHECKPOINT:   isCheckpoint = true,  isRecovering = false
// ROLLBACK:            isCheckpoint = false, isRecovering = false
// RECOVERY_CHECKPOINT: isCheckpoint = true,  isRecovering = true
WALReplayer::WALReplayer(WAL* wal, StorageManager* storageManager, BufferManager* bufferManager,
    Catalog* catalog, WALReplayMode replayMode)
    : isRecovering{replayMode == WALReplayMode::RECOVERY_CHECKPOINT},
      isCheckpoint{replayMode != WALReplayMode::ROLLBACK}, storageManager{storageManager},
      bufferManager{bufferManager}, wal{wal}, catalog{catalog} {
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
    case WALRecordType::OVERFLOW_FILE_NEXT_BYTE_POS_RECORD: {
        replayOverflowFileNextBytePosRecord(walRecord);
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

void WALReplayer::replayPageUpdateOrInsertRecord(const kuzu::storage::WALRecord& walRecord) {
    // 1. As the first step we copy over the page on disk, regardless of if we are recovering
    // (and checkpointing) or checkpointing while during regular execution.
    auto dbFileID = walRecord.pageInsertOrUpdateRecord.dbFileID;
    std::unique_ptr<FileInfo> fileInfoOfDBFile =
        StorageUtils::getFileInfoForReadWrite(wal->getDirectory(), dbFileID);
    if (isCheckpoint) {
        if (!wal->isLastLoggedRecordCommit()) {
            // Nothing to undo.
            return;
        }
        walFileHandle->readPage(pageBuffer.get(), walRecord.pageInsertOrUpdateRecord.pageIdxInWAL);
        FileUtils::writeToFile(fileInfoOfDBFile.get(), pageBuffer.get(),
            BufferPoolConstants::PAGE_4KB_SIZE,
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

void WALReplayer::replayTableStatisticsRecord(const kuzu::storage::WALRecord& walRecord) {
    if (isCheckpoint) {
        if (walRecord.tableStatisticsRecord.isNodeTable) {
            StorageUtils::overwriteNodesStatisticsAndDeletedIDsFileWithVersionFromWAL(
                wal->getDirectory());
            if (!isRecovering) {
                storageManager->getNodesStatisticsAndDeletedIDs()->checkpointInMemoryIfNecessary();
            }
        } else {
            StorageUtils::overwriteRelsStatisticsFileWithVersionFromWAL(wal->getDirectory());
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
        StorageUtils::overwriteCatalogFileWithVersionFromWAL(wal->getDirectory());
        if (!isRecovering) {
            catalog->checkpointInMemory();
        }
    } else {
        // Since DDL statements are single statements that are auto committed, it is impossible
        // for users to roll back a DDL statement.
    }
}

void WALReplayer::replayCreateTableRecord(const WALRecord& walRecord) {
    if (isCheckpoint) {
        // Since we log the NODE_TABLE_RECORD prior to logging CATALOG_RECORD, the catalog
        // file has not recovered yet. Thus, the catalog needs to read the catalog file for WAL
        // record.
        auto catalogForCheckpointing = getCatalogForRecovery(FileVersionType::WAL_VERSION);
        if (walRecord.copyTableRecord.tableType == TableType::NODE) {
            auto nodeTableSchema = reinterpret_cast<NodeTableSchema*>(
                catalogForCheckpointing->getReadOnlyVersion()->getTableSchema(
                    walRecord.createTableRecord.tableID));
            WALReplayerUtils::createEmptyHashIndexFiles(nodeTableSchema, wal->getDirectory());
        }
        if (!isRecovering) {
            // If we are not recovering, i.e., we are checkpointing during normal execution,
            // then we need to create the NodeTable object for the newly created node table.
            // Therefore, this effectively fixes the in-memory data structures (i.e., performs
            // the in-memory checkpointing).
            storageManager->createTable(
                walRecord.createTableRecord.tableID, catalogForCheckpointing.get());
        }
    } else {
        // Since DDL statements are single statements that are auto committed, it is
        // impossible for users to roll back a DDL statement.
    }
}

void WALReplayer::replayRdfGraphRecord(const WALRecord& walRecord) {
    if (isCheckpoint) {
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
    } else {
        // See comments for NODE_TABLE_RECORD.
    }
}

void WALReplayer::replayOverflowFileNextBytePosRecord(const WALRecord& walRecord) {
    // If we are recovering we do not replay OVERFLOW_FILE_NEXT_BYTE_POS_RECORD because
    // this record is intended for rolling back a transaction to ensure that we can
    // recover the overflow space allocated for the write transaction by calling
    // DiskOverflowFile::resetNextBytePosToWriteTo(...). However during recovery, storageManager
    // is null, so we cannot construct this value.
    if (isRecovering) {
        return;
    }
    KU_ASSERT(walRecord.diskOverflowFileNextBytePosRecord.dbFileID.isOverflow);
    auto dbFileID = walRecord.diskOverflowFileNextBytePosRecord.dbFileID;
    DiskOverflowFile* diskOverflowFile;
    switch (dbFileID.dbFileType) {
    case DBFileType::NODE_INDEX: {
        auto index = storageManager->getPKIndex(dbFileID.nodeIndexID.tableID);
        diskOverflowFile = index->getDiskOverflowFile();
    } break;
    default:
        throw RuntimeException("Unsupported dbFileID " + dbFileTypeToString(dbFileID.dbFileType) +
                               " for OVERFLOW_FILE_NEXT_BYTE_POS_RECORD.");
    }
    // Reset NextBytePosToWriteTo if we are rolling back.
    if (!isCheckpoint) {
        diskOverflowFile->resetNextBytePosToWriteTo(
            walRecord.diskOverflowFileNextBytePosRecord.prevNextBytePosToWriteTo);
    }
    diskOverflowFile->resetLoggedNewOverflowFileNextBytePosRecord();
}

void WALReplayer::replayCopyTableRecord(const kuzu::storage::WALRecord& walRecord) {
    auto tableID = walRecord.copyTableRecord.tableID;
    if (isCheckpoint) {
        if (!isRecovering) {
            // CHECKPOINT.
            // If we are not recovering, i.e., we are checkpointing during normal execution,
            // then we need to update the nodeTable because the actual columns and lists
            // files have been changed during checkpoint. So the in memory
            // fileHandles are obsolete and should be reconstructed (e.g. since the numPages
            // have likely changed they need to reconstruct their page locks).
            if (walRecord.copyTableRecord.tableType == TableType::NODE) {
                auto nodeTableSchema = reinterpret_cast<NodeTableSchema*>(
                    catalog->getReadOnlyVersion()->getTableSchema(tableID));
                storageManager->getNodeTable(tableID)->initializePKIndex(
                    nodeTableSchema, false /* readOnly */);
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

void WALReplayer::replayDropTableRecord(const kuzu::storage::WALRecord& walRecord) {
    if (isCheckpoint) {
        auto tableID = walRecord.dropTableRecord.tableID;
        if (!isRecovering) {
            auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(tableID);
            switch (tableSchema->getTableType()) {
            case TableType::NODE: {
                storageManager->dropTable(tableID);
                // TODO(Guodong): Do nothing for now. Should remove metaDA and reclaim free pages.
                WALReplayerUtils::removeHashIndexFile(
                    reinterpret_cast<NodeTableSchema*>(tableSchema), wal->getDirectory());
            } break;
            case TableType::REL: {
                storageManager->dropTable(tableID);
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
            auto catalogForRecovery = getCatalogForRecovery(FileVersionType::ORIGINAL);
            auto tableSchema = catalogForRecovery->getReadOnlyVersion()->getTableSchema(tableID);
            switch (tableSchema->getTableType()) {
            case TableType::NODE: {
                // TODO(Guodong): Do nothing for now. Should remove metaDA and reclaim free pages.
                WALReplayerUtils::removeHashIndexFile(
                    reinterpret_cast<NodeTableSchema*>(tableSchema), wal->getDirectory());
            } break;
            case TableType::REL: {
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

void WALReplayer::replayDropPropertyRecord(const kuzu::storage::WALRecord& walRecord) {
    if (isCheckpoint) {
        auto tableID = walRecord.dropPropertyRecord.tableID;
        auto propertyID = walRecord.dropPropertyRecord.propertyID;
        if (!isRecovering) {
            auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(tableID);
            switch (tableSchema->getTableType()) {
            case TableType::NODE: {
                storageManager->getNodeTable(tableID)->dropColumn(
                    tableSchema->getColumnID(propertyID));
                // TODO(Guodong): Do nothing for now. Should remove metaDA and reclaim free pages.
            } break;
            case TableType::REL: {
                storageManager->getRelTable(tableID)->dropColumn(
                    tableSchema->getColumnID(propertyID));
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

void WALReplayer::replayAddPropertyRecord(const kuzu::storage::WALRecord& walRecord) {
    auto tableID = walRecord.addPropertyRecord.tableID;
    auto propertyID = walRecord.addPropertyRecord.propertyID;
    if (!isCheckpoint) {
        auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(tableID);
        switch (tableSchema->getTableType()) {
        case TableType::NODE: {
            storageManager->getNodeTable(tableID)->dropColumn(tableSchema->getColumnID(propertyID));
        } break;
        case TableType::REL: {
            storageManager->getRelTable(tableID)->dropColumn(tableSchema->getColumnID(propertyID));
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
        return dbFileID.isOverflow ? index->getDiskOverflowFile()->getFileHandle() :
                                     index->getFileHandle();
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

std::unique_ptr<Catalog> WALReplayer::getCatalogForRecovery(FileVersionType fileVersionType) {
    // When we are recovering our database, the catalog field of walReplayer has not been
    // initialized and recovered yet. We need to create a new catalog to get node/rel tableSchemas
    // for recovering.
    auto catalogForRecovery = std::make_unique<Catalog>();
    catalogForRecovery->getReadOnlyVersion()->readFromFile(wal->getDirectory(), fileVersionType);
    return catalogForRecovery;
}

} // namespace storage
} // namespace kuzu
