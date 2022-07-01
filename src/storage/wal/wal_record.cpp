#include "src/storage/wal/include/wal_record.h"

namespace graphflow {
namespace storage {

StorageStructureID StorageStructureID::newStructuredNodePropertyColumnID(
    label_t nodeLabel, uint32_t propertyID, bool isOverflow) {
    StorageStructureID retVal;
    retVal.storageStructureType = STRUCTURED_NODE_PROPERTY_COLUMN;
    retVal.isOverflow = isOverflow;
    retVal.structuredNodePropertyColumnID.nodeLabel = nodeLabel;
    retVal.structuredNodePropertyColumnID.propertyID = propertyID;
    return retVal;
}

PageUpdateOrInsertRecord PageUpdateOrInsertRecord::newPageInsertOrUpdateRecord(
    StorageStructureID storageStructureID_, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL,
    bool isInsert) {
    PageUpdateOrInsertRecord retVal;
    retVal.storageStructureID = storageStructureID_;
    retVal.pageIdxInOriginalFile = pageIdxInOriginalFile;
    retVal.pageIdxInWAL = pageIdxInWAL;
    retVal.isInsert = isInsert;
    return retVal;
}

CommitRecord CommitRecord::newCommitRecord(uint64_t transactionID) {
    CommitRecord retVal;
    retVal.transactionID = transactionID;
    return retVal;
}

WALRecord WALRecord::newPageInsertOrUpdateRecord(StorageStructureID storageStructureID_,
    uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL, bool isInsert) {
    WALRecord retVal;
    retVal.recordType = PAGE_UPDATE_OR_INSERT_RECORD;
    retVal.pageInsertOrUpdateRecord = PageUpdateOrInsertRecord::newPageInsertOrUpdateRecord(
        storageStructureID_, pageIdxInOriginalFile, pageIdxInWAL, isInsert);
    return retVal;
}

WALRecord WALRecord::newPageUpdateRecord(
    StorageStructureID storageStructureID_, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL) {
    return WALRecord::newPageInsertOrUpdateRecord(
        storageStructureID_, pageIdxInOriginalFile, pageIdxInWAL, false /* is update */);
}

WALRecord WALRecord::newPageInsertRecord(
    StorageStructureID storageStructureID_, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL) {
    return WALRecord::newPageInsertOrUpdateRecord(
        storageStructureID_, pageIdxInOriginalFile, pageIdxInWAL, true /* is insert */);
}

WALRecord WALRecord::newCommitRecord(uint64_t transactionID) {
    WALRecord retVal;
    retVal.recordType = COMMIT_RECORD;
    retVal.commitRecord = CommitRecord::newCommitRecord(transactionID);
    return retVal;
}

WALRecord WALRecord::newNodeMetadataRecord() {
    WALRecord retVal;
    retVal.recordType = NODES_METADATA_RECORD;
    return retVal;
}

void WALRecord::constructWALRecordFromBytes(WALRecord& retVal, uint8_t* bytes, uint64_t& offset) {
    ((WALRecord*)&retVal)[0] = ((WALRecord*)(bytes + offset))[0];
    offset += sizeof(WALRecord);
}

void WALRecord::writeWALRecordToBytes(uint8_t* bytes, uint64_t& offset) {
    ((WALRecord*)(bytes + offset))[0] = *this;
    offset += sizeof(WALRecord);
}

} // namespace storage
} // namespace graphflow
