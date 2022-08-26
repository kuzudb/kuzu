#include "src/storage/include/wal_replayer.h"

#include "spdlog/spdlog.h"

#include "src/storage/include/storage_manager.h"
#include "src/storage/include/storage_utils.h"
#include "src/storage/include/wal_replayer_utils.h"

namespace graphflow {
namespace storage {

WALReplayer::WALReplayer(WAL* wal) : isRecovering{true}, isCheckpoint{true}, wal{wal} {
    init();
}

WALReplayer::WALReplayer(WAL* wal, StorageManager* storageManager, BufferManager* bufferManager,
    Catalog* catalog, bool isCheckpoint)
    : isRecovering{false}, isCheckpoint{isCheckpoint}, storageManager{storageManager},
      bufferManager{bufferManager}, wal{wal}, catalog{catalog} {
    init();
}

void WALReplayer::init() {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
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
    case NODES_METADATA_RECORD: {
        if (isCheckpoint) {
            StorageUtils::overwriteNodesMetadataFileWithVersionFromWAL(wal->getDirectory());
            if (!isRecovering) {
                storageManager->getNodesStore().getNodesMetadata().checkpointInMemoryIfNecessary();
            }
        } else {
            storageManager->getNodesStore().getNodesMetadata().rollbackInMemoryIfNecessary();
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
                // Therefore, this effectively fixe the in-memory data structures (i.e., performs
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
            auto nodesMetadataForCheckPointing = make_unique<NodesMetadata>(wal->getDirectory());
            auto maxNodeOffsetPerTable = nodesMetadataForCheckPointing->getMaxNodeOffsetPerTable();
            auto catalogForCheckPointing = make_unique<catalog::Catalog>(wal);
            catalogForCheckPointing->getReadOnlyVersion()->readFromFile(
                wal->getDirectory(), DBFileType::WAL_VERSION);
            WALReplayerUtils::createEmptyDBFilesForNewRelTable(catalogForCheckPointing.get(),
                walRecord.relTableRecord.tableID, wal->getDirectory(), maxNodeOffsetPerTable);
            if (!isRecovering) {
                // See comments for NODE_TABLE_RECORD.
                storageManager->getRelsStore().createRelTable(walRecord.nodeTableRecord.tableID,
                    maxNodeOffsetPerTable, bufferManager, wal, catalogForCheckPointing.get());
            }
        } else {
            // See comments for NODE_TABLE_RECORD.
        }
    } break;
    case OVERFLOW_FILE_NEXT_BYTE_POS_RECORD: {
        // If we are recovering we do not replay OVERFLOW_FILE_NEXT_BYTE_POS_RECORD because
        // this record is intended for rolling back a transaction to ensure that we can
        // recover the overflow space allocated for the write trx by calling
        // OverflowFile::resetNextBytePosToWriteTo(...). However during recovery, storageManager is
        // null, so we cannot construct this value.
        if (isRecovering) {
            return;
        }
        assert(walRecord.overflowFileNextBytePosRecord.storageStructureID.isOverflow);
        auto storageStructureID = walRecord.overflowFileNextBytePosRecord.storageStructureID;
        OverflowFile* overflowFile;
        switch (storageStructureID.storageStructureType) {
        case STRUCTURED_NODE_PROPERTY_COLUMN: {
            Column* column = storageManager->getNodesStore().getNodePropertyColumn(
                storageStructureID.structuredNodePropertyColumnID.tableID,
                storageStructureID.structuredNodePropertyColumnID.propertyID);
            // TODO: We are explicitly assuming that if the log record's storageStructureID is an
            // overflow file, then the storage structure is a StringPropertyColumn. This should
            // change as we support other variable length data types and in other storage
            // structures.
            overflowFile = reinterpret_cast<StringPropertyColumn*>(column)->getOverflowFile();
        } break;
        case LISTS: {
            assert(storageStructureID.listFileID.listType == UNSTRUCTURED_NODE_PROPERTY_LISTS);
            UnstructuredPropertyLists* unstructuredPropertyLists =
                storageManager->getNodesStore().getNodeUnstrPropertyLists(
                    storageStructureID.listFileID.unstructuredNodePropertyListsID.tableID);
            overflowFile = &unstructuredPropertyLists->overflowFile;
            assert(storageStructureID.listFileID.listFileType == BASE_LISTS);
        } break;
        default:
            throw RuntimeException("Unsupported storageStructureType " +
                                   to_string(storageStructureID.storageStructureType) +
                                   " for OVERFLOW_FILE_NEXT_BYTE_POS_RECORD.");
        }
        // Reset NextBytePosToWriteTo if we are rolling back.
        if (!isCheckpoint) {
            overflowFile->resetNextBytePosToWriteTo(
                walRecord.overflowFileNextBytePosRecord.prevNextBytePosToWriteTo);
        }
        overflowFile->resetLoggedNewOverflowFileNextBytePosRecord();
    } break;
    case COPY_NODE_CSV_RECORD: {
        if (isCheckpoint) {
            auto tableID = walRecord.copyNodeCsvRecord.tableID;
            if (!isRecovering) {
                auto nodeTableSchema = catalog->getReadOnlyVersion()->getNodeTableSchema(tableID);
                // If the WAL version of the file doesn't exist, we must have already replayed this
                // WAL and successfully replaced the original DB file and deleted the WAL version
                // but somehow WALReplayer must have failed/crashed before deleting the entire WAL
                // (which is why the log record is still here). In that case the renaming has
                // already happened, so we do not have to do anything, which is the behavior of
                // replaceNodeWithVersionFromWALIfExists, i.e., if the WAL version of the file does
                // not exists, it will not do anything.
                WALReplayerUtils::replaceNodeFilesWithVersionFromWALIfExists(
                    nodeTableSchema, wal->getDirectory());
                // If we are not recovering, i.e., we are checkpointing during normal execution,
                // then we need to update the nodeTable because the actual columns and lists files
                // have been changed during changed during checkpoint. So the in memory fileHandles
                // are obsolete and should be reconstructed (e.g. since the numPages have likely
                // changed they need to reconstruct their page locks).
                storageManager->getNodesStore().getNode(tableID)->loadColumnsAndListsFromDisk(
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
                storageManager->getRelsStore().getRel(tableID)->loadColumnsAndListsFromDisk(
                    *catalog,
                    storageManager->getNodesStore().getNodesMetadata().getMaxNodeOffsetPerTable(),
                    *bufferManager, wal);
                storageManager->getNodesStore().getNodesMetadata().setAdjListsAndColumns(
                    &storageManager->getRelsStore());
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
            // Update the page in buffer manager if it is in a frame. Note that we assume that
            // the pageBuffer currently contains the contents of the WALVersion, so the caller
            // needs to make sure that this assumption holds.
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
    case STRUCTURED_NODE_PROPERTY_COLUMN: {
        Column* column = storageManager->getNodesStore().getNodePropertyColumn(
            storageStructureID.structuredNodePropertyColumnID.tableID,
            storageStructureID.structuredNodePropertyColumnID.propertyID);
        // TODO: We are explicitly assuming that if the log record's storageStructureID is an
        // overflow file, then the storage structure is a StringPropertyColumn. This should
        // change as we support updates to other variable length data types in columns.
        return storageStructureID.isOverflow ?
                   reinterpret_cast<StringPropertyColumn*>(column)->getOverflowFileHandle() :
                   column->getFileHandle();
    }
    case LISTS: {
        switch (storageStructureID.listFileID.listType) {
        case UNSTRUCTURED_NODE_PROPERTY_LISTS: {
            UnstructuredPropertyLists* unstructuredPropertyLists =
                storageManager->getNodesStore().getNodeUnstrPropertyLists(
                    storageStructureID.listFileID.unstructuredNodePropertyListsID.tableID);
            switch (storageStructureID.listFileID.listFileType) {
            case BASE_LISTS: {
                return storageStructureID.isOverflow ?
                           unstructuredPropertyLists->overflowFile.getFileHandle() :
                           unstructuredPropertyLists->getFileHandle();
            }
            default:
                // Note: We do not clear the WAL version of updated pages (nor do we need to
                // update the Buffer Manager versions of these pages) for METADATA and HEADERS.
                // The reason is METADATA and HEADERs are stored in InMemoryDiskArrays, which
                // bypass the BufferManager and during checkpointing Lists, those InMemDiskArray
                // rely on the WAL version of these pages existing to update their own in-memory
                // versions manually. If we clear the WAL versions in WALReplayer
                // InMeMDiskArray::checkpointOrRollbackInMemoryIfNecessaryNoLock will omit
                // refreshing their in memory copies of these updated pages. If we make the
                // InMemDiskArrays also store their pages through the BufferManager, we should
                // return the VersionedFileHandle's for METADATA and HEADERs as well.
                return nullptr;
            }
        }
        default:
            throw RuntimeException(
                "There should not be any code path yet triggering getting List "
                "type name for anything other than UnstructuredNodePropertyList." +
                to_string(storageStructureID.listFileID.listType));
        }
    }
    default:
        throw RuntimeException("Unrecognized StorageStructureType inside "
                               "WALReplayer::replay(). StorageStructureType:" +
                               to_string(storageStructureID.storageStructureType));
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

    // We next perform an in-memory checkpointing or rolling back of any lists.
    for (auto& listFileID : wal->updatedUnstructuredPropertyLists) {
        switch (listFileID.listType) {
        case UNSTRUCTURED_NODE_PROPERTY_LISTS: {
            auto unstructuredPropertyLists =
                storageManager->getNodesStore().getNodeUnstrPropertyLists(
                    listFileID.unstructuredNodePropertyListsID.tableID);
            if (isCheckpoint) {
                unstructuredPropertyLists->checkpointInMemoryIfNecessary();
            } else {
                unstructuredPropertyLists->rollbackInMemoryIfNecessary();
            }
        } break;
        default:
            throw RuntimeException("Transactional updates to ListType: " +
                                   to_string(listFileID.listType) + " is not supported.");
        }
    }
}

} // namespace storage
} // namespace graphflow
