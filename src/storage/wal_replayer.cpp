#include "src/storage/include/wal_replayer.h"

#include "spdlog/spdlog.h"

#include "src/storage/include/storage_manager.h"

namespace graphflow {
namespace storage {

WALReplayer::WALReplayer(StorageManager* storageManager)
    : isRecovering{true}, isCheckpoint{true}, storageManager{storageManager} {
    init();
}

WALReplayer::WALReplayer(
    StorageManager* storageManager, BufferManager* bufferManager, bool isCheckpoint)
    : isRecovering{false}, isCheckpoint{isCheckpoint}, storageManager{storageManager},
      bufferManager{bufferManager} {
    init();
}

void WALReplayer::init() {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
    walFileHandle = WAL::createWALFileHandle(storageManager->getDBDirectory());
    pageBuffer = make_unique<uint8_t[]>(DEFAULT_PAGE_SIZE);
}
void WALReplayer::replayWALRecord(WALRecord& walRecord) {
    switch (walRecord.recordType) {
    case PAGE_UPDATE_OR_INSERT_RECORD: {
        // 1. As the first step we copy over the page on disk, regardless of if we are recovering
        // (and checkpointing) or checkpointing while during regular execution.
        auto storageStructureID = walRecord.pageInsertOrUpdateRecord.storageStructureID;
        unique_ptr<FileInfo> fileInfoOfStorageStructure = StorageUtils::getFileInfoForReadWrite(
            storageManager->getDBDirectory(), storageStructureID);
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
        // the WALVersion info for VersionedFileHandles.
        switch (storageStructureID.storageStructureType) {
        case STRUCTURED_NODE_PROPERTY_COLUMN: {
            checkpointOrRollbackInMemoryColumn(walRecord, storageStructureID);
        } break;
        case LISTS: {
            fileIDsofListsToCheckpointOrRollback.insert(storageStructureID.listFileID);
        } break;
        default:
            throw RuntimeException("Unrecognized StorageStructureType inside "
                                   "WALReplayer::replay(). StorageStructureType:" +
                                   to_string(storageStructureID.storageStructureType));
        }
    } break;
    case NODES_METADATA_RECORD: {
        if (isCheckpoint) {
            StorageUtils::overwriteNodesMetadataFileWithVersionFromWAL(
                storageManager->getDBDirectory());
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
            StorageUtils::overwriteCatalogFileWithVersionFromWAL(storageManager->getDBDirectory());
            storageManager->getCatalog()->checkpointInMemoryIfNecessary();
        } else {
            // Since DDL statements are single statements that are auto committed, it is impossible
            // for users to roll back a DDL statement.
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
    fileHandle->clearUpdatedWALPageVersionIfNecessary(
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
    if (!isRecovering && isCheckpoint && !storageManager->getWAL().isLastLoggedRecordCommit()) {
        throw StorageException(
            "Cannot checkpointWAL because last logged record is not a commit record.");
    }
    if (isRecovering && !storageManager->getWAL().isLastLoggedRecordCommit()) {
        logger->info("WALReplayer is in recovery mode but the last record is not commit, so not "
                     "replaying. This should not happen and the caller should instead not call "
                     "WALReplayer::replay.");
        throw StorageException("System should not try to rollback when the last logged record is "
                               "not a commit record.");
    }
    auto walIterator = storageManager->getWAL().getIterator();
    WALRecord walRecord;
    while (walIterator->hasNextRecord()) {
        walIterator->getNextRecord(walRecord);
        replayWALRecord(walRecord);
    }

    cout << "Num Lists found: " << fileIDsofListsToCheckpointOrRollback.size() << endl;
    // We next perform an in-memory checkpointing or rolling back of any lists.
    for (auto& listFileID : fileIDsofListsToCheckpointOrRollback) {
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
    fileIDsofListsToCheckpointOrRollback.clear();
    cout << "Done WALREPLAYER" << endl;
}

} // namespace storage
} // namespace graphflow
