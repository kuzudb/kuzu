#include "storage/wal_replayer.h"

#include "spdlog/spdlog.h"
#include "storage/storage_manager.h"
#include "storage/storage_utils.h"
#include "storage/wal_replayer_utils.h"

namespace kuzu {
namespace storage {

WALReplayer::WALReplayer(WAL* wal) : isRecovering{true}, isCheckpoint{true}, wal{wal} {
    init();
}

WALReplayer::WALReplayer(WAL* wal, StorageManager* storageManager, BufferManager* bufferManager,
    MemoryManager* memoryManager, Catalog* catalog, bool isCheckpoint)
    : isRecovering{false}, isCheckpoint{isCheckpoint}, storageManager{storageManager},
      bufferManager{bufferManager}, memoryManager{memoryManager}, wal{wal}, catalog{catalog} {
    init();
}

void WALReplayer::init() {
    logger = LoggerUtils::getOrCreateLogger("storage");
    walFileHandle = WAL::createWALFileHandle(wal->getDirectory());
    pageBuffer = make_unique<uint8_t[]>(DEFAULT_PAGE_SIZE);
}

void WALReplayer::replayWALRecord(WALRecord& walRecord) {
    switch (walRecord.recordType) {
    case PAGE_UPDATE_OR_INSERT_RECORD: {
        // 1. As the first step we copy over the page on disk, regardless of if we are recovering
        // (and checkpointing) or checkpointing while during regular execution.
        auto storageStructureID = walRecord.pageInsertOrUpdateRecord.storageStructureID;
        unique_ptr<FileInfo> fileInfoOfStorageStructure =
            StorageUtils::getFileInfoForReadWrite(wal->getDirectory(), storageStructureID);
        if (isCheckpoint) {
            walFileHandle->readPage(
                pageBuffer.get(), walRecord.pageInsertOrUpdateRecord.pageIdxInWAL);
            FileUtils::writeToFile(fileInfoOfStorageStructure.get(), pageBuffer.get(),
                DEFAULT_PAGE_SIZE,
                walRecord.pageInsertOrUpdateRecord.pageIdxInOriginalFile * DEFAULT_PAGE_SIZE);
        }
        if (!isRecovering) {
            // 2: If we are not recovering, we do any in-memory checkpointing or rolling back work
            // to make sure that the system's in-memory structures are consistent with what is on
            // disk. For example, we update the BM's image of the pages or InMemDiskArrays used by
            // lists or the WALVersion pageIdxs of pages for VersionedFileHandles.
            checkpointOrRollbackVersionedFileHandleAndBufferManager(walRecord, storageStructureID);
        }
    } break;
    case TABLE_STATISTICS_RECORD: {
        if (isCheckpoint) {
            if (walRecord.tableStatisticsRecord.isNodeTable) {
                StorageUtils::overwriteNodesStatisticsAndDeletedIDsFileWithVersionFromWAL(
                    wal->getDirectory());
                if (!isRecovering) {
                    storageManager->getNodesStore()
                        .getNodesStatisticsAndDeletedIDs()
                        .checkpointInMemoryIfNecessary();
                }
            } else {
                StorageUtils::overwriteRelsStatisticsFileWithVersionFromWAL(wal->getDirectory());
                if (!isRecovering) {
                    storageManager->getRelsStore()
                        .getRelsStatistics()
                        .checkpointInMemoryIfNecessary();
                }
            }
        } else {
            storageManager->getNodesStore()
                .getNodesStatisticsAndDeletedIDs()
                .rollbackInMemoryIfNecessary();
        }
    } break;
    case COMMIT_RECORD: {
    } break;
    case CATALOG_RECORD: {
        if (isCheckpoint) {
            StorageUtils::overwriteCatalogFileWithVersionFromWAL(wal->getDirectory());
            if (!isRecovering) {
                storageManager->getCatalog()->checkpointInMemoryIfNecessary();
            }
        } else {
            // Since DDL statements are single statements that are auto committed, it is impossible
            // for users to roll back a DDL statement.
        }
    } break;
    case NODE_TABLE_RECORD: {
        if (isCheckpoint) {
            // Since we log the NODE_TABLE_RECORD prior to logging CATALOG_RECORD, the catalog
            // file has not recovered yet. Thus, the catalog needs to read the catalog file for WAL
            // record.
            auto catalogForCheckpointing = make_unique<catalog::Catalog>(wal);
            catalogForCheckpointing->getReadOnlyVersion()->readFromFile(
                wal->getDirectory(), DBFileType::WAL_VERSION);
            WALReplayerUtils::createEmptyDBFilesForNewNodeTable(catalogForCheckpointing.get(),
                walRecord.nodeTableRecord.tableID, wal->getDirectory());
            if (!isRecovering) {
                // If we are not recovering, i.e., we are checkpointing during normal execution,
                // then we need to create the NodeTable object for the newly created node table.
                // Therefore, this effectively fixes the in-memory data structures (i.e., performs
                // the in-memory checkpointing).
                storageManager->getNodesStore().createNodeTable(walRecord.nodeTableRecord.tableID,
                    bufferManager, wal, catalogForCheckpointing.get());
            }
        } else {
            // Since DDL statements are single statements that are auto committed, it is
            // impossible for users to roll back a DDL statement.
        }
    } break;
    case REL_TABLE_RECORD: {
        if (isCheckpoint) {
            // See comments for NODE_TABLE_RECORD.
            auto nodesStatisticsAndDeletedIDsForCheckPointing =
                make_unique<NodesStatisticsAndDeletedIDs>(wal->getDirectory());
            auto maxNodeOffsetPerTable =
                nodesStatisticsAndDeletedIDsForCheckPointing->getMaxNodeOffsetPerTable();
            auto catalogForCheckPointing = make_unique<catalog::Catalog>(wal);
            catalogForCheckPointing->getReadOnlyVersion()->readFromFile(
                wal->getDirectory(), DBFileType::WAL_VERSION);
            WALReplayerUtils::createEmptyDBFilesForNewRelTable(catalogForCheckPointing.get(),
                walRecord.relTableRecord.tableID, wal->getDirectory(), maxNodeOffsetPerTable);
            if (!isRecovering) {
                // See comments for NODE_TABLE_RECORD.
                storageManager->getRelsStore().createRelTable(walRecord.nodeTableRecord.tableID,
                    bufferManager, wal, catalogForCheckPointing.get(), memoryManager);
            }
        } else {
            // See comments for NODE_TABLE_RECORD.
        }
    } break;
    case OVERFLOW_FILE_NEXT_BYTE_POS_RECORD: {
        // If we are recovering we do not replay OVERFLOW_FILE_NEXT_BYTE_POS_RECORD because
        // this record is intended for rolling back a transaction to ensure that we can
        // recover the overflow space allocated for the write trx by calling
        // DiskOverflowFile::resetNextBytePosToWriteTo(...). However during recovery, storageManager
        // is null, so we cannot construct this value.
        if (isRecovering) {
            return;
        }
        assert(walRecord.diskOverflowFileNextBytePosRecord.storageStructureID.isOverflow);
        auto storageStructureID = walRecord.diskOverflowFileNextBytePosRecord.storageStructureID;
        DiskOverflowFile* diskOverflowFile;
        switch (storageStructureID.storageStructureType) {
        case COLUMN: {
            switch (storageStructureID.columnFileID.columnType) {
            case STRUCTURED_NODE_PROPERTY_COLUMN: {
                Column* column = storageManager->getNodesStore().getNodePropertyColumn(
                    storageStructureID.columnFileID.structuredNodePropertyColumnID.tableID,
                    storageStructureID.columnFileID.structuredNodePropertyColumnID.propertyID);
                diskOverflowFile =
                    reinterpret_cast<PropertyColumnWithOverflow*>(column)->getDiskOverflowFile();
            } break;
            case REL_PROPERTY_COLUMN: {
                auto& relNodeTableAndDir =
                    storageStructureID.columnFileID.relPropertyColumnID.relNodeTableAndDir;
                Column* column =
                    storageManager->getRelsStore().getRelPropertyColumn(relNodeTableAndDir.dir,
                        relNodeTableAndDir.relTableID, relNodeTableAndDir.srcNodeTableID,
                        storageStructureID.columnFileID.relPropertyColumnID.propertyID);
                diskOverflowFile =
                    reinterpret_cast<PropertyColumnWithOverflow*>(column)->getDiskOverflowFile();
            } break;
            default:
                throw RuntimeException(
                    "AdjColumn shouldn't have OVERFLOW_FILE_NEXT_BYTE_POS_RECORD.");
            }
        } break;
        case LISTS: {
            switch (storageStructureID.listFileID.listType) {
            case REL_PROPERTY_LISTS: {
                auto& relNodeTableAndDir =
                    storageStructureID.listFileID.relPropertyListID.relNodeTableAndDir;
                auto relPropertyLists =
                    storageManager->getRelsStore().getRelPropertyLists(relNodeTableAndDir.dir,
                        relNodeTableAndDir.srcNodeTableID, relNodeTableAndDir.relTableID,
                        storageStructureID.listFileID.relPropertyListID.propertyID);
                diskOverflowFile =
                    &((PropertyListsWithOverflow*)relPropertyLists)->diskOverflowFile;
            } break;
            default:
                throw RuntimeException(
                    "AdjLists shouldn't have OVERFLOW_FILE_NEXT_BYTE_POS_RECORD.");
                assert(storageStructureID.listFileID.listFileType == BASE_LISTS);
            }
        } break;
        case NODE_INDEX: {
            auto index =
                storageManager->getNodesStore().getPKIndex(storageStructureID.nodeIndexID.tableID);
            diskOverflowFile = index->getDiskOverflowFile();
        } break;
        default:
            throw RuntimeException("Unsupported storageStructureType " +
                                   to_string(storageStructureID.storageStructureType) +
                                   " for OVERFLOW_FILE_NEXT_BYTE_POS_RECORD.");
        }
        // Reset NextBytePosToWriteTo if we are rolling back.
        if (!isCheckpoint) {
            diskOverflowFile->resetNextBytePosToWriteTo(
                walRecord.diskOverflowFileNextBytePosRecord.prevNextBytePosToWriteTo);
        }
        diskOverflowFile->resetLoggedNewOverflowFileNextBytePosRecord();
    } break;
    case COPY_NODE_CSV_RECORD: {
        if (isCheckpoint) {
            auto tableID = walRecord.copyNodeCsvRecord.tableID;
            if (!isRecovering) {
                auto nodeTableSchema = catalog->getReadOnlyVersion()->getNodeTableSchema(tableID);
                // If the WAL version of the file doesn't exist, we must have already replayed
                // this WAL and successfully replaced the original DB file and deleted the WAL
                // version but somehow WALReplayer must have failed/crashed before deleting the
                // entire WAL (which is why the log record is still here). In that case the
                // renaming has already happened, so we do not have to do anything, which is the
                // behavior of replaceNodeWithVersionFromWALIfExists, i.e., if the WAL version
                // of the file does not exists, it will not do anything.
                WALReplayerUtils::replaceNodeFilesWithVersionFromWALIfExists(
                    nodeTableSchema, wal->getDirectory());
                // If we are not recovering, i.e., we are checkpointing during normal execution,
                // then we need to update the nodeTable because the actual columns and lists
                // files have been changed during checkpoint. So the in memory
                // fileHandles are obsolete and should be reconstructed (e.g. since the numPages
                // have likely changed they need to reconstruct their page locks).
                storageManager->getNodesStore().getNodeTable(tableID)->loadColumnsAndListsFromDisk(
                    nodeTableSchema, *bufferManager, wal);
            } else {
                auto catalogForCheckpointing = make_unique<catalog::Catalog>();
                catalogForCheckpointing->getReadOnlyVersion()->readFromFile(
                    wal->getDirectory(), DBFileType::ORIGINAL);
                // See comments above.
                WALReplayerUtils::replaceNodeFilesWithVersionFromWALIfExists(
                    catalogForCheckpointing->getReadOnlyVersion()->getNodeTableSchema(tableID),
                    wal->getDirectory());
            }
        } else {
            // Since COPY_CSV statements are single statements that are auto committed, it is
            // impossible for users to roll back a COPY_CSV statement.
        }
    } break;
    case COPY_REL_CSV_RECORD: {
        if (isCheckpoint) {
            auto tableID = walRecord.copyRelCsvRecord.tableID;
            if (!isRecovering) {
                // See comments for COPY_NODE_CSV_RECORD.
                WALReplayerUtils::replaceRelPropertyFilesWithVersionFromWALIfExists(
                    catalog->getReadOnlyVersion()->getRelTableSchema(tableID), wal->getDirectory(),
                    catalog);
                // See comments for COPY_NODE_CSV_RECORD.
                storageManager->getRelsStore().getRelTable(tableID)->loadColumnsAndListsFromDisk(
                    *catalog, *bufferManager);
                storageManager->getNodesStore()
                    .getNodesStatisticsAndDeletedIDs()
                    .setAdjListsAndColumns(&storageManager->getRelsStore());
            } else {
                auto catalogForCheckpointing = make_unique<catalog::Catalog>();
                catalogForCheckpointing->getReadOnlyVersion()->readFromFile(
                    wal->getDirectory(), DBFileType::ORIGINAL);
                // See comments for COPY_NODE_CSV_RECORD.
                WALReplayerUtils::replaceRelPropertyFilesWithVersionFromWALIfExists(
                    catalogForCheckpointing->getReadOnlyVersion()->getRelTableSchema(tableID),
                    wal->getDirectory(), catalogForCheckpointing.get());
            }
        } else {
            // See comments for COPY_NODE_CSV_RECORD.
        }
    } break;
    case DROP_TABLE_RECORD: {
        if (isCheckpoint) {
            auto tableID = walRecord.dropTableRecord.tableID;
            if (!isRecovering) {
                if (walRecord.dropTableRecord.isNodeTable) {
                    storageManager->getNodesStore().removeNodeTable(tableID);
                    WALReplayerUtils::removeDBFilesForNodeTable(
                        catalog->getReadOnlyVersion()->getNodeTableSchema(tableID),
                        wal->getDirectory());
                } else {
                    storageManager->getRelsStore().removeRelTable(tableID);
                    WALReplayerUtils::removeDBFilesForRelTable(
                        catalog->getReadOnlyVersion()->getRelTableSchema(tableID),
                        wal->getDirectory(), catalog);
                }
            } else {
                auto catalogForCheckpointing = make_unique<catalog::Catalog>();
                catalogForCheckpointing->getReadOnlyVersion()->readFromFile(
                    wal->getDirectory(), DBFileType::ORIGINAL);
                if (walRecord.dropTableRecord.isNodeTable) {
                    WALReplayerUtils::removeDBFilesForNodeTable(
                        catalogForCheckpointing->getReadOnlyVersion()->getNodeTableSchema(tableID),
                        wal->getDirectory());
                } else {
                    WALReplayerUtils::removeDBFilesForRelTable(
                        catalogForCheckpointing->getReadOnlyVersion()->getRelTableSchema(tableID),
                        wal->getDirectory(), catalogForCheckpointing.get());
                }
            }
        } else {
            // See comments for COPY_NODE_CSV_RECORD.
        }
    } break;
    default:
        throw RuntimeException(
            "Unrecognized WAL record type inside WALReplayer::replay. recordType: " +
            to_string(walRecord.recordType));
    }
}

void WALReplayer::truncateFileIfInsertion(
    VersionedFileHandle* fileHandle, const PageUpdateOrInsertRecord& pageInsertOrUpdateRecord) {
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
    const WALRecord& walRecord, const StorageStructureID& storageStructureID) {
    VersionedFileHandle* fileHandle =
        getVersionedFileHandleIfWALVersionAndBMShouldBeCleared(storageStructureID);
    if (fileHandle) {
        fileHandle->clearWALPageVersionIfNecessary(
            walRecord.pageInsertOrUpdateRecord.pageIdxInOriginalFile);
        if (isCheckpoint) {
            // Update the page in buffer manager if it is in a frame. Note that we assume
            // that the pageBuffer currently contains the contents of the WALVersion, so the
            // caller needs to make sure that this assumption holds.
            bufferManager->updateFrameIfPageIsInFrameWithoutPageOrFrameLock(*fileHandle,
                pageBuffer.get(), walRecord.pageInsertOrUpdateRecord.pageIdxInOriginalFile);
        } else {
            truncateFileIfInsertion(fileHandle, walRecord.pageInsertOrUpdateRecord);
        }
    }
}

VersionedFileHandle* WALReplayer::getVersionedFileHandleIfWALVersionAndBMShouldBeCleared(
    const StorageStructureID& storageStructureID) {
    switch (storageStructureID.storageStructureType) {
    case COLUMN: {
        switch (storageStructureID.columnFileID.columnType) {
        case STRUCTURED_NODE_PROPERTY_COLUMN: {
            Column* column = storageManager->getNodesStore().getNodePropertyColumn(
                storageStructureID.columnFileID.structuredNodePropertyColumnID.tableID,
                storageStructureID.columnFileID.structuredNodePropertyColumnID.propertyID);
            return storageStructureID.isOverflow ?
                       reinterpret_cast<PropertyColumnWithOverflow*>(column)
                           ->getDiskOverflowFileHandle() :
                       column->getFileHandle();
        }
        case ADJ_COLUMN: {
            auto& relNodeTableAndDir =
                storageStructureID.columnFileID.adjColumnID.relNodeTableAndDir;
            Column* column = storageManager->getRelsStore().getAdjColumn(relNodeTableAndDir.dir,
                relNodeTableAndDir.srcNodeTableID, relNodeTableAndDir.relTableID);
            return column->getFileHandle();
        }
        case REL_PROPERTY_COLUMN: {
            auto& relNodeTableAndDir =
                storageStructureID.columnFileID.relPropertyColumnID.relNodeTableAndDir;
            Column* column =
                storageManager->getRelsStore().getRelPropertyColumn(relNodeTableAndDir.dir,
                    relNodeTableAndDir.relTableID, relNodeTableAndDir.srcNodeTableID,
                    storageStructureID.columnFileID.relPropertyColumnID.propertyID);
            return storageStructureID.isOverflow ?
                       reinterpret_cast<PropertyColumnWithOverflow*>(column)
                           ->getDiskOverflowFileHandle() :
                       column->getFileHandle();
        }
        default: {
            assert(false);
        }
        }
    }
    case LISTS: {
        switch (storageStructureID.listFileID.listType) {
        case ADJ_LISTS: {
            auto& relNodeTableAndDir = storageStructureID.listFileID.adjListsID.relNodeTableAndDir;
            auto adjLists = storageManager->getRelsStore().getAdjLists(relNodeTableAndDir.dir,
                relNodeTableAndDir.srcNodeTableID, relNodeTableAndDir.relTableID);
            switch (storageStructureID.listFileID.listFileType) {
            case BASE_LISTS: {
                return adjLists->getFileHandle();
            }
            default:
                return nullptr;
            }
        }
        case REL_PROPERTY_LISTS: {
            auto& relNodeTableAndDir =
                storageStructureID.listFileID.relPropertyListID.relNodeTableAndDir;
            auto relPropLists =
                storageManager->getRelsStore().getRelPropertyLists(relNodeTableAndDir.dir,
                    relNodeTableAndDir.srcNodeTableID, relNodeTableAndDir.relTableID,
                    storageStructureID.listFileID.relPropertyListID.propertyID);
            switch (storageStructureID.listFileID.listFileType) {
            case BASE_LISTS: {
                return storageStructureID.isOverflow ? ((PropertyListsWithOverflow*)relPropLists)
                                                           ->diskOverflowFile.getFileHandle() :
                                                       relPropLists->getFileHandle();
            }
            default:
                return nullptr;
            }
        }
        default: {
            assert(false);
        }
        }
    }
    case NODE_INDEX: {
        auto index =
            storageManager->getNodesStore().getPKIndex(storageStructureID.nodeIndexID.tableID);
        return storageStructureID.isOverflow ? index->getDiskOverflowFile()->getFileHandle() :
                                               index->getFileHandle();
    }
    default:
        assert(false);
    }
}

void WALReplayer::replay() {
    // Note: We assume no other thread is accessing the wal during the following operations.
    // If this assumption no longer holds, we need to lock the wal.
    if (!isRecovering && isCheckpoint && !wal->isLastLoggedRecordCommit()) {
        throw StorageException(
            "Cannot checkpointWAL because last logged record is not a commit record.");
    }
    if (isRecovering && !wal->isLastLoggedRecordCommit()) {
        logger->info("WALReplayer is in recovery mode but the last record is not commit, so not "
                     "replaying. This should not happen and the caller should instead not call "
                     "WALReplayer::replay.");
        throw StorageException("System should not try to rollback when the last logged record is "
                               "not a commit record.");
    }
    auto walIterator = wal->getIterator();
    WALRecord walRecord;
    while (walIterator->hasNextRecord()) {
        walIterator->getNextRecord(walRecord);
        replayWALRecord(walRecord);
    }

    // We next perform an in-memory checkpointing or rolling back of nodeTables.
    for (auto nodeTableID : wal->updatedNodeTables) {
        auto nodeTable = storageManager->getNodesStore().getNodeTable(nodeTableID);
        if (isCheckpoint) {
            nodeTable->checkpointInMemoryIfNecessary();
        } else {
            nodeTable->rollbackInMemoryIfNecessary();
        }
    }
    // Then we perform an in-memory checkpointing or rolling back of relTables.
    for (auto relTableID : wal->updatedRelTables) {
        auto relTable = storageManager->getRelsStore().getRelTable(relTableID);
        if (isCheckpoint) {
            relTable->checkpointInMemoryIfNecessary();
        } else {
            relTable->rollbackInMemoryIfNecessary();
        }
    }
}

} // namespace storage
} // namespace kuzu
