#include "storage/wal_replayer.h"

#include "storage/storage_manager.h"
#include "storage/storage_utils.h"
#include "storage/wal_replayer_utils.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

WALReplayer::WALReplayer(WAL* wal) : isRecovering{true}, isCheckpoint{true}, wal{wal} {
    init();
}

WALReplayer::WALReplayer(WAL* wal, StorageManager* storageManager, MemoryManager* memoryManager,
    Catalog* catalog, bool isCheckpoint)
    : isRecovering{false}, isCheckpoint{isCheckpoint}, storageManager{storageManager},
      bufferManager{memoryManager->getBufferManager()},
      memoryManager{memoryManager}, wal{wal}, catalog{catalog} {
    init();
}

void WALReplayer::init() {
    logger = LoggerUtils::getLogger(LoggerConstants::LoggerEnum::STORAGE);
    walFileHandle = wal->fileHandle;
    pageBuffer = std::make_unique<uint8_t[]>(BufferPoolConstants::PAGE_4KB_SIZE);
}

void WALReplayer::replay() {
    // Note: We assume no other thread is accessing the wal during the following operations.
    // If this assumption no longer holds, we need to lock the wal.
    if (!isRecovering && isCheckpoint && !wal->isLastLoggedRecordCommit()) {
        throw StorageException(
            "Cannot checkpoint WAL because last logged record is not a commit record.");
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

void WALReplayer::replayWALRecord(WALRecord& walRecord) {
    switch (walRecord.recordType) {
    case WALRecordType::PAGE_UPDATE_OR_INSERT_RECORD: {
        // 1. As the first step we copy over the page on disk, regardless of if we are recovering
        // (and checkpointing) or checkpointing while during regular execution.
        auto storageStructureID = walRecord.pageInsertOrUpdateRecord.storageStructureID;
        std::unique_ptr<FileInfo> fileInfoOfStorageStructure =
            StorageUtils::getFileInfoForReadWrite(wal->getDirectory(), storageStructureID);
        if (isCheckpoint) {
            walFileHandle->readPage(
                pageBuffer.get(), walRecord.pageInsertOrUpdateRecord.pageIdxInWAL);
            FileUtils::writeToFile(fileInfoOfStorageStructure.get(), pageBuffer.get(),
                BufferPoolConstants::PAGE_4KB_SIZE,
                walRecord.pageInsertOrUpdateRecord.pageIdxInOriginalFile *
                    BufferPoolConstants::PAGE_4KB_SIZE);
        }
        if (!isRecovering) {
            // 2: If we are not recovering, we do any in-memory checkpointing or rolling back work
            // to make sure that the system's in-memory structures are consistent with what is on
            // disk. For example, we update the BM's image of the pages or InMemDiskArrays used by
            // lists or the WALVersion pageIdxs of pages for VersionedFileHandles.
            checkpointOrRollbackVersionedFileHandleAndBufferManager(walRecord, storageStructureID);
        }
    } break;
    case WALRecordType::TABLE_STATISTICS_RECORD: {
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
    case WALRecordType::COMMIT_RECORD: {
    } break;
    case WALRecordType::CATALOG_RECORD: {
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
    case WALRecordType::NODE_TABLE_RECORD: {
        if (isCheckpoint) {
            // Since we log the NODE_TABLE_RECORD prior to logging CATALOG_RECORD, the catalog
            // file has not recovered yet. Thus, the catalog needs to read the catalog file for WAL
            // record.
            auto catalogForCheckpointing = getCatalogForRecovery(DBFileType::WAL_VERSION);
            WALReplayerUtils::createEmptyDBFilesForNewNodeTable(
                catalogForCheckpointing->getReadOnlyVersion()->getNodeTableSchema(
                    walRecord.nodeTableRecord.tableID),
                wal->getDirectory());
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
    case WALRecordType::REL_TABLE_RECORD: {
        if (isCheckpoint) {
            // See comments for NODE_TABLE_RECORD.
            auto nodesStatisticsAndDeletedIDsForCheckPointing =
                std::make_unique<NodesStatisticsAndDeletedIDs>(wal->getDirectory());
            auto maxNodeOffsetPerTable =
                nodesStatisticsAndDeletedIDsForCheckPointing->getMaxNodeOffsetPerTable();
            auto catalogForCheckpointing = getCatalogForRecovery(DBFileType::WAL_VERSION);
            WALReplayerUtils::createEmptyDBFilesForNewRelTable(
                catalogForCheckpointing->getReadOnlyVersion()->getRelTableSchema(
                    walRecord.relTableRecord.tableID),
                wal->getDirectory(), maxNodeOffsetPerTable);
            if (!isRecovering) {
                // See comments for NODE_TABLE_RECORD.
                storageManager->getRelsStore().createRelTable(walRecord.nodeTableRecord.tableID,
                    bufferManager, wal, catalogForCheckpointing.get(), memoryManager);
                storageManager->getNodesStore()
                    .getNodesStatisticsAndDeletedIDs()
                    .setAdjListsAndColumns(&storageManager->getRelsStore());
            }
        } else {
            // See comments for NODE_TABLE_RECORD.
        }
    } break;
    case WALRecordType::OVERFLOW_FILE_NEXT_BYTE_POS_RECORD: {
        // If we are recovering we do not replay OVERFLOW_FILE_NEXT_BYTE_POS_RECORD because
        // this record is intended for rolling back a transaction to ensure that we can
        // recover the overflow space allocated for the write transaction by calling
        // DiskOverflowFile::resetNextBytePosToWriteTo(...). However during recovery, storageManager
        // is null, so we cannot construct this value.
        if (isRecovering) {
            return;
        }
        assert(walRecord.diskOverflowFileNextBytePosRecord.storageStructureID.isOverflow);
        auto storageStructureID = walRecord.diskOverflowFileNextBytePosRecord.storageStructureID;
        DiskOverflowFile* diskOverflowFile;
        switch (storageStructureID.storageStructureType) {
        case StorageStructureType::COLUMN: {
            switch (storageStructureID.columnFileID.columnType) {
            case ColumnType::NODE_PROPERTY_COLUMN: {
                Column* column = storageManager->getNodesStore().getNodePropertyColumn(
                    storageStructureID.columnFileID.nodePropertyColumnID.tableID,
                    storageStructureID.columnFileID.nodePropertyColumnID.propertyID);
                diskOverflowFile =
                    reinterpret_cast<PropertyColumnWithOverflow*>(column)->getDiskOverflowFile();
            } break;
            case ColumnType::REL_PROPERTY_COLUMN: {
                auto& relNodeTableAndDir =
                    storageStructureID.columnFileID.relPropertyColumnID.relNodeTableAndDir;
                Column* column = storageManager->getRelsStore().getRelPropertyColumn(
                    relNodeTableAndDir.dir, relNodeTableAndDir.relTableID,
                    storageStructureID.columnFileID.relPropertyColumnID.propertyID);
                diskOverflowFile =
                    reinterpret_cast<PropertyColumnWithOverflow*>(column)->getDiskOverflowFile();
            } break;
            default:
                throw RuntimeException(
                    "AdjColumn shouldn't have OVERFLOW_FILE_NEXT_BYTE_POS_RECORD.");
            }
        } break;
        case StorageStructureType::LISTS: {
            switch (storageStructureID.listFileID.listType) {
            case ListType::REL_PROPERTY_LISTS: {
                auto& relNodeTableAndDir =
                    storageStructureID.listFileID.relPropertyListID.relNodeTableAndDir;
                auto relPropertyLists = storageManager->getRelsStore().getRelPropertyLists(
                    relNodeTableAndDir.dir, relNodeTableAndDir.relTableID,
                    storageStructureID.listFileID.relPropertyListID.propertyID);
                diskOverflowFile =
                    &((PropertyListsWithOverflow*)relPropertyLists)->diskOverflowFile;
            } break;
            default:
                throw RuntimeException(
                    "AdjLists shouldn't have OVERFLOW_FILE_NEXT_BYTE_POS_RECORD.");
                assert(storageStructureID.listFileID.listFileType == ListFileType::BASE_LISTS);
            }
        } break;
        case StorageStructureType::NODE_INDEX: {
            auto index =
                storageManager->getNodesStore().getPKIndex(storageStructureID.nodeIndexID.tableID);
            diskOverflowFile = index->getDiskOverflowFile();
        } break;
        default:
            throw RuntimeException(
                "Unsupported storageStructureType " +
                storageStructureTypeToString(storageStructureID.storageStructureType) +
                " for OVERFLOW_FILE_NEXT_BYTE_POS_RECORD.");
        }
        // Reset NextBytePosToWriteTo if we are rolling back.
        if (!isCheckpoint) {
            diskOverflowFile->resetNextBytePosToWriteTo(
                walRecord.diskOverflowFileNextBytePosRecord.prevNextBytePosToWriteTo);
        }
        diskOverflowFile->resetLoggedNewOverflowFileNextBytePosRecord();
    } break;
    case WALRecordType::COPY_NODE_RECORD: {
        if (isCheckpoint) {
            auto tableID = walRecord.copyNodeRecord.tableID;
            if (!isRecovering) {
                auto nodeTableSchema = catalog->getReadOnlyVersion()->getNodeTableSchema(tableID);
                // If the WAL version of the file doesn't exist, we must have already replayed
                // this WAL and successfully replaced the original DB file and deleted the WAL
                // version but somehow WALReplayer must have failed/crashed before deleting the
                // entire WAL (which is why the log record is still here). In that case the
                // renaming has already happened, so we do not have to do anything, which is the
                // behavior of replaceNodeWithVersionFromWALIfExists, i.e., if the WAL version
                // of the file does not exist, it will not do anything.
                WALReplayerUtils::replaceNodeFilesWithVersionFromWALIfExists(
                    nodeTableSchema, wal->getDirectory());
                auto relTableSchemas = catalog->getAllRelTableSchemasContainBoundTable(tableID);
                WALReplayerUtils::replaceListsHeadersFilesWithVersionFromWALIfExists(
                    relTableSchemas, tableID, wal->getDirectory());
                // If we are not recovering, i.e., we are checkpointing during normal execution,
                // then we need to update the nodeTable because the actual columns and lists
                // files have been changed during checkpoint. So the in memory
                // fileHandles are obsolete and should be reconstructed (e.g. since the numPages
                // have likely changed they need to reconstruct their page locks).
                storageManager->getNodesStore().getNodeTable(tableID)->initializeData(
                    nodeTableSchema);
                for (auto relTableSchema : relTableSchemas) {
                    storageManager->getRelsStore()
                        .getRelTable(relTableSchema->tableID)
                        ->initializeData(relTableSchema);
                }
            } else {
                auto catalogForRecovery = getCatalogForRecovery(DBFileType::ORIGINAL);
                // See comments above.
                WALReplayerUtils::replaceNodeFilesWithVersionFromWALIfExists(
                    catalogForRecovery->getReadOnlyVersion()->getNodeTableSchema(tableID),
                    wal->getDirectory());
                WALReplayerUtils::replaceListsHeadersFilesWithVersionFromWALIfExists(
                    catalogForRecovery->getAllRelTableSchemasContainBoundTable(tableID), tableID,
                    wal->getDirectory());
            }
        } else {
            // Since COPY_CSV statements are single statements that are auto committed, it is
            // impossible for users to roll back a COPY_CSV statement.
        }
    } break;
    case WALRecordType::COPY_REL_RECORD: {
        if (isCheckpoint) {
            auto tableID = walRecord.copyRelRecord.tableID;
            if (!isRecovering) {
                // See comments for COPY_NODE_RECORD.
                WALReplayerUtils::replaceRelPropertyFilesWithVersionFromWALIfExists(
                    catalog->getReadOnlyVersion()->getRelTableSchema(tableID), wal->getDirectory());
                // See comments for COPY_NODE_RECORD.
                storageManager->getRelsStore().getRelTable(tableID)->initializeData(
                    catalog->getReadOnlyVersion()->getRelTableSchema(tableID));
                storageManager->getNodesStore()
                    .getNodesStatisticsAndDeletedIDs()
                    .setAdjListsAndColumns(&storageManager->getRelsStore());
            } else {
                auto catalogForRecovery = getCatalogForRecovery(DBFileType::ORIGINAL);
                // See comments for COPY_NODE_RECORD.
                WALReplayerUtils::replaceRelPropertyFilesWithVersionFromWALIfExists(
                    catalogForRecovery->getReadOnlyVersion()->getRelTableSchema(tableID),
                    wal->getDirectory());
            }
        } else {
            // See comments for COPY_NODE_RECORD.
        }
    } break;
    case WALRecordType::DROP_TABLE_RECORD: {
        if (isCheckpoint) {
            auto tableID = walRecord.dropTableRecord.tableID;
            if (!isRecovering) {
                if (catalog->getReadOnlyVersion()->containNodeTable(tableID)) {
                    storageManager->getNodesStore().removeNodeTable(tableID);
                    WALReplayerUtils::removeDBFilesForNodeTable(
                        catalog->getReadOnlyVersion()->getNodeTableSchema(tableID),
                        wal->getDirectory());
                } else {
                    storageManager->getRelsStore().removeRelTable(tableID);
                    WALReplayerUtils::removeDBFilesForRelTable(
                        catalog->getReadOnlyVersion()->getRelTableSchema(tableID),
                        wal->getDirectory());
                }
            } else {
                auto catalogForRecovery = getCatalogForRecovery(DBFileType::ORIGINAL);
                if (catalogForRecovery->getReadOnlyVersion()->containNodeTable(tableID)) {
                    WALReplayerUtils::removeDBFilesForNodeTable(
                        catalogForRecovery->getReadOnlyVersion()->getNodeTableSchema(tableID),
                        wal->getDirectory());
                } else {
                    WALReplayerUtils::removeDBFilesForRelTable(
                        catalogForRecovery->getReadOnlyVersion()->getRelTableSchema(tableID),
                        wal->getDirectory());
                }
            }
        } else {
            // See comments for COPY_NODE_RECORD.
        }
    } break;
    case WALRecordType::DROP_PROPERTY_RECORD: {
        if (isCheckpoint) {
            auto tableID = walRecord.dropPropertyRecord.tableID;
            auto propertyID = walRecord.dropPropertyRecord.propertyID;
            if (!isRecovering) {
                if (catalog->getReadOnlyVersion()->containNodeTable(tableID)) {
                    WALReplayerUtils::removeDBFilesForNodeProperty(
                        wal->getDirectory(), tableID, propertyID);
                    storageManager->getNodesStore().getNodeTable(tableID)->removeProperty(
                        propertyID);
                } else {
                    WALReplayerUtils::removeDBFilesForRelProperty(wal->getDirectory(),
                        catalog->getReadOnlyVersion()->getRelTableSchema(tableID), propertyID);
                    storageManager->getRelsStore().getRelTable(tableID)->removeProperty(
                        propertyID, *catalog->getReadOnlyVersion()->getRelTableSchema(tableID));
                }
            } else {
                auto catalogForRecovery = getCatalogForRecovery(DBFileType::WAL_VERSION);
                if (catalogForRecovery->getReadOnlyVersion()->containNodeTable(tableID)) {
                    WALReplayerUtils::removeDBFilesForNodeProperty(
                        wal->getDirectory(), tableID, propertyID);
                } else {
                    WALReplayerUtils::removeDBFilesForRelProperty(wal->getDirectory(),
                        catalogForRecovery->getReadOnlyVersion()->getRelTableSchema(tableID),
                        propertyID);
                }
            }
        } else {
            // See comments for COPY_NODE_RECORD.
        }
    } break;
    case WALRecordType::ADD_PROPERTY_RECORD: {
        if (isCheckpoint) {
            auto tableID = walRecord.addPropertyRecord.tableID;
            auto propertyID = walRecord.addPropertyRecord.propertyID;
            if (!isRecovering) {
                auto tableSchema = catalog->getWriteVersion()->getTableSchema(tableID);
                auto property = tableSchema->getProperty(propertyID);
                if (catalog->getReadOnlyVersion()->containNodeTable(tableID)) {
                    WALReplayerUtils::renameDBFilesForNodeProperty(
                        wal->getDirectory(), tableID, propertyID);
                    storageManager->getNodesStore().getNodeTable(tableID)->addProperty(property);
                } else {
                    WALReplayerUtils::renameDBFilesForRelProperty(wal->getDirectory(),
                        reinterpret_cast<RelTableSchema*>(tableSchema), propertyID);
                    storageManager->getRelsStore().getRelTable(tableID)->addProperty(
                        property, *reinterpret_cast<RelTableSchema*>(tableSchema));
                }
            } else {
                auto catalogForRecovery = getCatalogForRecovery(DBFileType::WAL_VERSION);
                auto tableSchema =
                    catalogForRecovery->getReadOnlyVersion()->getTableSchema(tableID);
                auto property = tableSchema->getProperty(propertyID);
                if (catalogForRecovery->getReadOnlyVersion()->containNodeTable(tableID)) {
                    WALReplayerUtils::renameDBFilesForNodeProperty(
                        wal->getDirectory(), tableID, propertyID);
                } else {
                    WALReplayerUtils::renameDBFilesForRelProperty(wal->getDirectory(),
                        reinterpret_cast<RelTableSchema*>(tableSchema), propertyID);
                }
            }
        } else {
            // See comments for COPY_NODE_RECORD.
        }
    } break;
    default:
        throw RuntimeException(
            "Unrecognized WAL record type inside WALReplayer::replay. recordType: " +
            walRecordTypeToString(walRecord.recordType));
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
    const WALRecord& walRecord, const StorageStructureID& storageStructureID) {
    BMFileHandle* fileHandle =
        getVersionedFileHandleIfWALVersionAndBMShouldBeCleared(storageStructureID);
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
    const StorageStructureID& storageStructureID) {
    switch (storageStructureID.storageStructureType) {
    case StorageStructureType::COLUMN: {
        switch (storageStructureID.columnFileID.columnType) {
        case ColumnType::NODE_PROPERTY_COLUMN: {
            Column* column = storageManager->getNodesStore().getNodePropertyColumn(
                storageStructureID.columnFileID.nodePropertyColumnID.tableID,
                storageStructureID.columnFileID.nodePropertyColumnID.propertyID);
            return storageStructureID.isOverflow ?
                       reinterpret_cast<PropertyColumnWithOverflow*>(column)
                           ->getDiskOverflowFileHandle() :
                       column->getFileHandle();
        }
        case ColumnType::ADJ_COLUMN: {
            auto& relNodeTableAndDir =
                storageStructureID.columnFileID.adjColumnID.relNodeTableAndDir;
            Column* column = storageManager->getRelsStore().getAdjColumn(
                relNodeTableAndDir.dir, relNodeTableAndDir.relTableID);
            return column->getFileHandle();
        }
        case ColumnType::REL_PROPERTY_COLUMN: {
            auto& relNodeTableAndDir =
                storageStructureID.columnFileID.relPropertyColumnID.relNodeTableAndDir;
            Column* column = storageManager->getRelsStore().getRelPropertyColumn(
                relNodeTableAndDir.dir, relNodeTableAndDir.relTableID,
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
    case StorageStructureType::LISTS: {
        switch (storageStructureID.listFileID.listType) {
        case ListType::ADJ_LISTS: {
            auto& relNodeTableAndDir = storageStructureID.listFileID.adjListsID.relNodeTableAndDir;
            auto adjLists = storageManager->getRelsStore().getAdjLists(
                relNodeTableAndDir.dir, relNodeTableAndDir.relTableID);
            switch (storageStructureID.listFileID.listFileType) {
            case ListFileType::BASE_LISTS: {
                return adjLists->getFileHandle();
            }
            default:
                return nullptr;
            }
        }
        case ListType::REL_PROPERTY_LISTS: {
            auto& relNodeTableAndDir =
                storageStructureID.listFileID.relPropertyListID.relNodeTableAndDir;
            auto relPropLists = storageManager->getRelsStore().getRelPropertyLists(
                relNodeTableAndDir.dir, relNodeTableAndDir.relTableID,
                storageStructureID.listFileID.relPropertyListID.propertyID);
            switch (storageStructureID.listFileID.listFileType) {
            case ListFileType::BASE_LISTS: {
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
    case StorageStructureType::NODE_INDEX: {
        auto index =
            storageManager->getNodesStore().getPKIndex(storageStructureID.nodeIndexID.tableID);
        return storageStructureID.isOverflow ? index->getDiskOverflowFile()->getFileHandle() :
                                               index->getFileHandle();
    }
    default:
        assert(false);
    }
}

std::unique_ptr<Catalog> WALReplayer::getCatalogForRecovery(DBFileType dbFileType) {
    // When we are recovering our database, the catalog field of walReplayer has not been
    // initialized and recovered yet. We need to create a new catalog to get node/rel tableSchemas
    // for recovering.
    auto catalogForRecovery = std::make_unique<Catalog>(wal);
    catalogForRecovery->getReadOnlyVersion()->readFromFile(wal->getDirectory(), dbFileType);
    return catalogForRecovery;
}

} // namespace storage
} // namespace kuzu
