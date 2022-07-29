#include "src/storage/include/wal_replayer.h"

#include "spdlog/spdlog.h"

#include "src/catalog/include/catalog.h"
#include "src/storage/include/storage_manager.h"
#include "src/storage/include/storage_utils.h"

namespace graphflow {
namespace storage {

WALReplayer::WALReplayer(WAL* wal) : isRecovering{true}, isCheckpoint{true}, wal{wal} {
    init();
}

WALReplayer::WALReplayer(
    WAL* wal, StorageManager* storageManager, BufferManager* bufferManager, bool isCheckpoint)
    : isRecovering{false}, isCheckpoint{isCheckpoint}, storageManager{storageManager},
      bufferManager{bufferManager}, wal{wal} {
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
        // If we are recovering there is nothing else to do and we can return
        if (isRecovering) {
            return;
        }

        // 2: If we are not recovering, we do any in-memory checkpointing or rolling back work to
        // make sure that the system's in-memory structures are consistent with what is on disk.
        // For example, we update the BM's image of the pages or InMemDiskArrays used by lists or
        // the WALVersion pageIdxs of pages for VersionedFileHandles.
        switch (storageStructureID.storageStructureType) {
        case STRUCTURED_NODE_PROPERTY_COLUMN: {
            checkpointOrRollbackInMemoryColumn(walRecord, storageStructureID);
        } break;
        case LISTS: {
            fileIDsOfListsToCheckpointOrRollback.insert(storageStructureID.listFileID);
        } break;
        default:
            throw RuntimeException("Unrecognized StorageStructureType inside "
                                   "WALReplayer::replay(). StorageStructureType:" +
                                   to_string(storageStructureID.storageStructureType));
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
            auto catalogForCheckpointing = make_unique<catalog::Catalog>();
            catalogForCheckpointing->getReadOnlyVersion()->readFromFile(
                wal->getDirectory(), true /* isForWALRecord */);
            NodeTable::createEmptyDBFilesForNewNodeTable(catalogForCheckpointing.get(),
                walRecord.nodeTableRecord.labelID, wal->getDirectory());
            if (!isRecovering) {
                // If we are not recovering, i.e., we are checkpointing during normal execution,
                // then we need to create the NodeTable object for the newly created node table.
                // Therefore, this effectively fixe the in-memory data structures (i.e., performs
                // the in-memory checkpointing).
                storageManager->getNodesStore().createNodeTable(walRecord.nodeTableRecord.labelID,
                    bufferManager, wal, catalogForCheckpointing.get());
            }
        } else {
            // Since DDL statements are single statements that are auto committed, it is
            // impossible for users to roll back a DDL statement.
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

void WALReplayer::checkpointOrRollbackInMemoryColumn(
    const WALRecord& walRecord, const StorageStructureID& storageStructureID) {
    Column* column = storageManager->getNodesStore().getNodePropertyColumn(
        storageStructureID.structuredNodePropertyColumnID.nodeLabel,
        storageStructureID.structuredNodePropertyColumnID.propertyID);
    // TODO: We are explicitly assuming that if the log record's storageStructureID is an
    // overflow file, then the storage structure is a StringPropertyColumn. This should change
    // as we support other variable length data types and in other storage structures.
    VersionedFileHandle* fileHandle =
        storageStructureID.isOverflow ?
            reinterpret_cast<StringPropertyColumn*>(column)->getOverflowFileHandle() :
            column->getFileHandle();
    fileHandle->clearWALPageVersionIfNecessary(
        walRecord.pageInsertOrUpdateRecord.pageIdxInOriginalFile);
    if (isCheckpoint) {
        // Update the page in buffer manager if it is in a frame
        bufferManager->updateFrameIfPageIsInFrameWithoutPageOrFrameLock(*fileHandle,
            pageBuffer.get(), walRecord.pageInsertOrUpdateRecord.pageIdxInOriginalFile);
    } else {
        truncateFileIfInsertion(fileHandle, walRecord.pageInsertOrUpdateRecord);
    }
}

void WALReplayer::replay() {
    // Note: We assume no other thread is accessing the wal during the following operations. If
    // this assumption no longer holds, we need to lock the wal.
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
    for (auto& listFileID : fileIDsOfListsToCheckpointOrRollback) {
        switch (listFileID.listType) {
        case UNSTRUCTURED_NODE_PROPERTY_LISTS: {
            auto unstructuredPropertyLists =
                storageManager->getNodesStore().getNodeUnstrPropertyLists(
                    listFileID.unstructuredNodePropertyListsID.nodeLabel);
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
    // Strictly speaking this is not necessary because WALReplayer should not be used
    // multiple times but we clear any fileIDs we accumulated during replay for
    // checkpointing or rolling back when we are done with replaying.
    fileIDsOfListsToCheckpointOrRollback.clear();
}

} // namespace storage
} // namespace graphflow
