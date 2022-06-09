#include "src/storage/include/wal_replayer.h"

#include "src/storage/include/storage_manager.h"

namespace graphflow {
namespace storage {

WALReplayer::WALReplayer(
    StorageManager& storageManager, BufferManager& bufferManager, bool isCheckpoint)
    : storageManager{storageManager}, bufferManager{bufferManager},
      walFileHandle{storageManager.getWAL().fileHandle}, isCheckpoint{isCheckpoint} {
    pageBuffer = make_unique<uint8_t[]>(DEFAULT_PAGE_SIZE);
}

void WALReplayer::replayWALRecord(WALRecord& walRecord) {
    switch (walRecord.recordType) {
    case PAGE_UPDATE_OR_INSERT_RECORD: {
        Column* column;
        auto storageStructureID = walRecord.pageInsertOrUpdateRecord.storageStructureID;
        switch (storageStructureID.storageStructureType) {
        case STRUCTURED_NODE_PROPERTY_COLUMN: {
            column = storageManager.getNodesStore().getNodePropertyColumn(
                storageStructureID.structuredNodePropertyColumnID.nodeLabel,
                storageStructureID.structuredNodePropertyColumnID.propertyID);
        } break;
        default:
            throw RuntimeException("Unrecognized StorageStructureType inside "
                                   "WALReplayer::replay(). StorageStructureType:" +
                                   to_string(storageStructureID.storageStructureType));
        }
        // TODO(Semih/Guodong): We are explicitly assuming that if the log record's
        // storageStructureID is an overflow file, then the storage structure is a
        // StringPropertyColumn. This should change as we support other variable length data types
        // and in other storage structures.
        VersionedFileHandle* fileHandle =
            storageStructureID.isOverflow ?
                reinterpret_cast<StringPropertyColumn*>(column)->getOverflowFileHandle() :
                column->getFileHandle();
        if (isCheckpoint) {
            walFileHandle->readPage(
                pageBuffer.get(), walRecord.pageInsertOrUpdateRecord.pageIdxInWAL);
            fileHandle->writePage(
                pageBuffer.get(), walRecord.pageInsertOrUpdateRecord.pageIdxInOriginalFile);
            // Update the page in buffer manager if it is in a frame
            bufferManager.updateFrameIfPageIsInFrameWithoutPageOrFrameLock(*fileHandle,
                pageBuffer.get(), walRecord.pageInsertOrUpdateRecord.pageIdxInOriginalFile);
        }
        if (!isCheckpoint && walRecord.pageInsertOrUpdateRecord.isInsert) {
            // Note: We can directly call removePageIdxAndTruncateIfNecessary here because we assume
            // there is a single write transaction in the system at any point in time. Suppose page
            // 5 and page 6 were added to a file during a transaction, which is now rolling back. As
            // we replay the log to rollback, we see page 5's insertion first. However, we can
            // directly truncate the file to 5 (even if later we will see a page 6 insertion),
            // because we assume that if there were further new page additions to the same file,
            // they must also be part of the rolling back transaction. If this assumption fails,
            // instead of truncating, we need to indicate that page 5 is a "free" page again and
            // have some logic to maintain free pages.
            fileHandle->removePageIdxAndTruncateIfNecessary(
                walRecord.pageInsertOrUpdateRecord.pageIdxInOriginalFile);
        } else {
            fileHandle->clearUpdatedWALPageVersion(
                walRecord.pageInsertOrUpdateRecord.pageIdxInOriginalFile);
        }
    } break;
    case NODES_METADATA_RECORD: {
        cout << "Starting to replay NODES_METADATA_RECORD. isCheckpoint: "
             << (isCheckpoint ? " true" : " false") << endl;
        if (isCheckpoint) {
            StorageUtils::overwriteNodesMetadataFileWithVersionFromWAL(
                storageManager.getDBDirectory());
            cout << "Called overwriteNodesMetadataFileWithVersionFromWAL()." << endl;
            storageManager.getNodesStore().getNodesMetadata().commitIfNecessary();
            cout << "Called .getNodesMetadata().commitIfNecessary()." << endl;
        } else {
            StorageUtils::removeNodesMetadataFileForWALIfExists(storageManager.getDBDirectory());
            storageManager.getNodesStore().getNodesMetadata().rollbackIfNecessary();
        }
    } break;
    case COMMIT_RECORD: {
    } break;
    default:
        throw RuntimeException(
            "Unrecognized WAL record type inside WALReplayer::replay. recordType: " +
            to_string(walRecord.recordType));
    }
}

void WALReplayer::replay() {
    // Note: We assume no other thread is accessing the wal during the following operations. If this
    // assumption no longer holds, we need to lock the wal.
    if (isCheckpoint && !storageManager.getWAL().isLastLoggedRecordCommit()) {
        throw StorageException(
            "Cannot checkpointWAL because last logged record is not a commit record.");
    }
    auto walIterator = storageManager.getWAL().getIterator();
    WALRecord walRecord;
    while (walIterator->hasNextRecord()) {
        walIterator->getNextRecord(walRecord);
        replayWALRecord(walRecord);
    }
}

} // namespace storage
} // namespace graphflow
