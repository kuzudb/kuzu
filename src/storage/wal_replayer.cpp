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
    case PAGE_UPDATE_RECORD: {
        Column* column;
        auto fileID = walRecord.pageUpdateRecord.fileID;
        switch (fileID.fileIDType) {
        case STRUCTURED_NODE_PROPERTY_FILE_ID: {
            column = storageManager.getNodesStore().getNodePropertyColumn(
                fileID.structuredNodePropFileID.nodeLabel,
                fileID.structuredNodePropFileID.propertyID);
        } break;
        case ADJ_COLUMN_PROPERTY_FILE_ID: {
            column = storageManager.getRelsStore().getRelPropertyColumn(
                fileID.adjColumnPropertyFileID.relLabel, fileID.adjColumnPropertyFileID.nodeLabel,
                fileID.adjColumnPropertyFileID.propertyID);
        } break;
        default:
            throw RuntimeException(
                "Unrecognized FileIDType inside WALReplayer::replay(). FileIDType:" +
                to_string(fileID.fileIDType));
        }
        if (isCheckpoint) {
            // First update the original db file on disk
            FileHandle* fileHandleToWriteTo = column->getFileHandle();
            walFileHandle->readPage(pageBuffer.get(), walRecord.pageUpdateRecord.pageIdxInWAL);
            fileHandleToWriteTo->writePage(
                pageBuffer.get(), walRecord.pageUpdateRecord.pageIdxInOriginalFile);
            // Update the page in buffer manager if it is in a frame
            bufferManager.updateFrameIfPageIsInFrameWithoutPageOrFrameLock(*fileHandleToWriteTo,
                pageBuffer.get(), walRecord.pageUpdateRecord.pageIdxInOriginalFile);
        }
        column->clearPageVersionInfo(walRecord.pageUpdateRecord.pageIdxInOriginalFile);
    }
    case COMMIT_RECORD: {
        break;
    }
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
    uint64_t i = 0;
    while (walIterator->hasNextRecord()) {
        walIterator->getNextRecord(walRecord);
        replayWALRecord(walRecord);
    }
}

} // namespace storage
} // namespace graphflow
