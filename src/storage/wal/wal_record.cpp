#include "src/storage/wal/include/wal_record.h"

namespace graphflow {
namespace storage {

void StructuredNodePropertyColumnID::constructStructuredNodePropertyColumnIDFromBytes(
    StructuredNodePropertyColumnID& retVal, uint8_t* bytes, uint64_t& offset) {
    retVal.nodeLabel = ((label_t*)(bytes + offset))[0];
    offset += sizeof(label_t);
    retVal.propertyID = ((uint32_t*)(bytes + offset))[0];
    offset += 4;
};

void StructuredNodePropertyColumnID::writeStructuredNodePropertyColumnIDToBytes(
    uint8_t* bytes, uint64_t& offset) {
    ((label_t*)(bytes + offset))[0] = nodeLabel;
    offset += sizeof(label_t);
    ((uint32_t*)(bytes + offset))[0] = propertyID;
    offset += 4;
}

StorageStructureID StorageStructureID::newStructuredNodePropertyColumnID(
    label_t nodeLabel, uint32_t propertyID, bool isOverflow) {
    StorageStructureID retVal;
    retVal.storageStructureType = STRUCTURED_NODE_PROPERTY_COLUMN;
    retVal.isOverflow = isOverflow;
    retVal.structuredNodePropertyColumnID.nodeLabel = nodeLabel;
    retVal.structuredNodePropertyColumnID.propertyID = propertyID;
    return retVal;
}

void StorageStructureID::constructStorageStructureIDFromBytes(
    StorageStructureID& retVal, uint8_t* bytes, uint64_t& offset) {
    retVal.storageStructureType =
        static_cast<StorageStructureType>(bytes[offset++]); // 1 byte for StorageStructureType
    retVal.isOverflow = ((bool*)(bytes + offset++))[0];     // 1 byte for isOverflow
    switch (retVal.storageStructureType) {
    case STRUCTURED_NODE_PROPERTY_COLUMN: {
        StructuredNodePropertyColumnID::constructStructuredNodePropertyColumnIDFromBytes(
            retVal.structuredNodePropertyColumnID, bytes, offset);
    } break;
    default: {
        throw RuntimeException("Unrecognized StorageStructureType when reading WAL record in "
                               "constructStorageStructureIDFromBytes(). StorageStructureType:" +
                               to_string(retVal.storageStructureType));
    }
    }
}

void StorageStructureID::writeStorageStructureIDToBytes(uint8_t* bytes, uint64_t& offset) {
    bytes[offset++] = storageStructureType;
    bytes[offset++] = isOverflow;
    switch (storageStructureType) {
    case STRUCTURED_NODE_PROPERTY_COLUMN: {
        structuredNodePropertyColumnID.writeStructuredNodePropertyColumnIDToBytes(bytes, offset);
    } break;
    default: {
        throw RuntimeException("Unrecognized StorageStructureType when reading WAL record in "
                               "writeStorageStructureIDToBytes(). StorageStructureType:" +
                               to_string(storageStructureType));
    }
    }
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

void PageUpdateOrInsertRecord::constructPageUpdateOrInsertRecordFromBytes(
    PageUpdateOrInsertRecord& retVal, uint8_t* bytes, uint64_t& offset) {
    StorageStructureID::constructStorageStructureIDFromBytes(
        retVal.storageStructureID, bytes, offset);
    retVal.pageIdxInOriginalFile = ((uint64_t*)(bytes + offset))[0];
    retVal.pageIdxInWAL = ((uint64_t*)(bytes + offset))[1];
    offset += 2 * sizeof(uint64_t);
    retVal.isInsert = ((bool*)(bytes + offset))[0];
    offset += sizeof(bool);
}

void PageUpdateOrInsertRecord::writePageUpdateOrInsertRecordToBytes(
    uint8_t* bytes, uint64_t& offset) {
    storageStructureID.writeStorageStructureIDToBytes(bytes, offset);
    ((uint64_t*)(bytes + offset))[0] = pageIdxInOriginalFile;
    ((uint64_t*)(bytes + offset))[1] = pageIdxInWAL;
    offset += 2 * sizeof(uint64_t);
    ((bool*)(bytes + offset))[0] = isInsert;
    offset += sizeof(bool);
}

CommitRecord CommitRecord::newCommitRecord(uint64_t transactionID) {
    CommitRecord retVal;
    retVal.transactionID = transactionID;
    return retVal;
}

void CommitRecord::constructCommitRecordFromBytes(
    CommitRecord& retVal, uint8_t* bytes, uint64_t& offset) {
    retVal.transactionID = ((uint64_t*)(bytes + offset))[0];
    offset += sizeof(uint64_t);
}

void CommitRecord::writeCommitRecordToBytes(uint8_t* bytes, uint64_t& offset) {
    ((uint64_t*)(bytes + offset))[0] = transactionID;
    offset += sizeof(uint64_t);
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

void WALRecord::constructWALRecordFromBytes(WALRecord& retVal, uint8_t* bytes, uint64_t& offset) {
    retVal.recordType = static_cast<WALRecordType>(bytes[offset++]);
    switch (retVal.recordType) {
    case PAGE_UPDATE_OR_INSERT_RECORD: {
        PageUpdateOrInsertRecord::constructPageUpdateOrInsertRecordFromBytes(
            retVal.pageInsertOrUpdateRecord, bytes, offset);
    } break;
    case COMMIT_RECORD: {
        CommitRecord::constructCommitRecordFromBytes(retVal.commitRecord, bytes, offset);
    } break;
    default: {
        throw RuntimeException(
            "Unrecognized WAL record type in constructWALRecordFromBytes(). recordType: " +
            to_string(retVal.recordType));
    }
    }
}

void WALRecord::writeWALRecordToBytes(uint8_t* bytes, uint64_t& offset) {
    bytes[offset++] = recordType;
    switch (recordType) {
    case PAGE_UPDATE_OR_INSERT_RECORD: {
        pageInsertOrUpdateRecord.writePageUpdateOrInsertRecordToBytes(bytes, offset);
    } break;
    case COMMIT_RECORD: {
        commitRecord.writeCommitRecordToBytes(bytes, offset);
    } break;
    default: {
        throw RuntimeException(
            "Unrecognized WAL record type in writeWALRecordToBytes(). recordType: " +
            to_string(recordType));
    }
    }
}

} // namespace storage
} // namespace graphflow
