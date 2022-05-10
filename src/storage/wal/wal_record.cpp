#include "src/storage/include/wal/wal_record.h"

namespace graphflow {
namespace storage {

void StructuredNodePropertyFileID::constructStructuredNodePropertyFileIDFromBytes(
    StructuredNodePropertyFileID& retVal, uint8_t* bytes, uint64_t& offset) {
    retVal.nodeLabel = ((label_t*)(bytes + offset))[0];
    offset += sizeof(label_t);
    retVal.propertyID = ((uint32_t*)(bytes + offset))[0];
    offset += 4;
};

void StructuredNodePropertyFileID::writeStructuredNodePropertyFileIDToBytes(
    uint8_t* bytes, uint64_t& offset) {
    ((label_t*)(bytes + offset))[0] = nodeLabel;
    offset += sizeof(label_t);
    ((uint32_t*)(bytes + offset))[0] = propertyID;
    offset += 4;
}

void AdjColumnPropertyFileID::constructAdjColumnPropertyFileIDFromBytes(
    AdjColumnPropertyFileID& retVal, uint8_t* bytes, uint64_t& offset) {
    retVal.nodeLabel = ((label_t*)(bytes + offset))[0];
    offset += sizeof(label_t);
    retVal.relLabel = ((label_t*)(bytes + offset))[0];
    offset += sizeof(label_t);
    retVal.propertyID = ((uint32_t*)(bytes + offset))[0];
    offset += sizeof(uint32_t);
};

void AdjColumnPropertyFileID::writeAdjColumnPropertyFileIDToBytes(
    uint8_t* bytes, uint64_t& offset) {
    ((label_t*)(bytes + offset))[0] = nodeLabel;
    offset += sizeof(label_t);
    ((label_t*)(bytes + offset))[0] = relLabel;
    offset += sizeof(label_t);
    ((uint32_t*)(bytes + offset))[0] = propertyID;
    offset += sizeof(uint32_t);
}

FileID FileID::newStructuredNodePropertyFileID(label_t nodeLabel, uint32_t propertyID) {
    FileID retVal;
    retVal.fileIDType = STRUCTURED_NODE_PROPERTY_FILE_ID;
    retVal.structuredNodePropFileID.nodeLabel = nodeLabel;
    retVal.structuredNodePropFileID.propertyID = propertyID;
    return retVal;
}

FileID FileID::newStructuredAdjColumnPropertyFileID(
    label_t nodeLabel, label_t relLabel, uint32_t propertyID) {
    FileID retVal;
    retVal.fileIDType = ADJ_COLUMN_PROPERTY_FILE_ID;
    retVal.adjColumnPropertyFileID.nodeLabel = nodeLabel;
    retVal.adjColumnPropertyFileID.relLabel = relLabel;
    retVal.adjColumnPropertyFileID.propertyID = propertyID;
    return retVal;
}

void FileID::constructFileIDFromBytes(FileID& retVal, uint8_t* bytes, uint64_t& offset) {
    retVal.fileIDType = static_cast<FileIDType>(bytes[offset++]); // 1 byte for FileIDType
    switch (retVal.fileIDType) {
    case STRUCTURED_NODE_PROPERTY_FILE_ID: {
        StructuredNodePropertyFileID::constructStructuredNodePropertyFileIDFromBytes(
            retVal.structuredNodePropFileID, bytes, offset);
    } break;
    case ADJ_COLUMN_PROPERTY_FILE_ID: {
        AdjColumnPropertyFileID::constructAdjColumnPropertyFileIDFromBytes(
            retVal.adjColumnPropertyFileID, bytes, offset);
    } break;
    default: {
        throw RuntimeException("Unrecognized FileIDType when reading WAL record in "
                               "constructFileIDFromBytes(). FileIDType:" +
                               to_string(retVal.fileIDType));
    }
    }
}

void FileID::writeFileIDToBytes(uint8_t* bytes, uint64_t& offset) {
    bytes[offset++] = fileIDType;
    switch (fileIDType) {
    case STRUCTURED_NODE_PROPERTY_FILE_ID: {
        structuredNodePropFileID.writeStructuredNodePropertyFileIDToBytes(bytes, offset);
    } break;
    case ADJ_COLUMN_PROPERTY_FILE_ID: {
        adjColumnPropertyFileID.writeAdjColumnPropertyFileIDToBytes(bytes, offset);
    } break;
    default: {
        throw RuntimeException("Unrecognized FileIDType when reading WAL record in "
                               "writeFileIDToBytes(). FileIDType:" +
                               to_string(fileIDType));
    }
    }
}

PageUpdateRecord PageUpdateRecord::newStructuredNodePropertyPageUpdateRecord(
    label_t nodeLabel, uint32_t propertyID, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL) {
    PageUpdateRecord retVal;
    retVal.fileID = FileID::newStructuredNodePropertyFileID(nodeLabel, propertyID);
    retVal.pageIdxInOriginalFile = pageIdxInOriginalFile;
    retVal.pageIdxInWAL = pageIdxInWAL;
    return retVal;
}

PageUpdateRecord PageUpdateRecord::newStructuredAdjColumnPropertyPageUpdateRecord(label_t nodeLabel,
    label_t relLabel, uint32_t propertyID, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL) {
    PageUpdateRecord retVal;
    retVal.fileID = FileID::newStructuredNodePropertyFileID(nodeLabel, propertyID);
    retVal.pageIdxInOriginalFile = pageIdxInOriginalFile;
    retVal.pageIdxInWAL = pageIdxInWAL;
    return retVal;
}

void PageUpdateRecord::constructPageUpdateRecordFromBytes(
    PageUpdateRecord& retVal, uint8_t* bytes, uint64_t& offset) {
    FileID::constructFileIDFromBytes(retVal.fileID, bytes, offset);
    retVal.pageIdxInOriginalFile = ((uint64_t*)(bytes + offset))[0];
    retVal.pageIdxInWAL = ((uint64_t*)(bytes + offset))[1];
    offset += 2 * sizeof(uint64_t);
}

void PageUpdateRecord::writePageUpdateLogToBytes(uint8_t* bytes, uint64_t& offset) {
    fileID.writeFileIDToBytes(bytes, offset);
    ((uint64_t*)(bytes + offset))[0] = pageIdxInOriginalFile;
    ((uint64_t*)(bytes + offset))[1] = pageIdxInWAL;
    offset += 2 * sizeof(uint64_t);
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
WALRecord WALRecord::newStructuredNodePropertyPageUpdateRecord(
    label_t nodeLabel, uint32_t propertyID, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL) {
    WALRecord retVal;
    retVal.recordType = PAGE_UPDATE_RECORD;
    retVal.pageUpdateRecord = PageUpdateRecord::newStructuredNodePropertyPageUpdateRecord(
        nodeLabel, propertyID, pageIdxInOriginalFile, pageIdxInWAL);
    return retVal;
}

WALRecord WALRecord::newStructuredAdjColumnPropertyPageUpdateRecord(label_t nodeLabel,
    label_t relLabel, uint32_t propertyID, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL) {
    WALRecord retVal;
    retVal.recordType = PAGE_UPDATE_RECORD;
    retVal.pageUpdateRecord = PageUpdateRecord::newStructuredNodePropertyPageUpdateRecord(
        nodeLabel, propertyID, pageIdxInOriginalFile, pageIdxInWAL);
    return retVal;
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
    case PAGE_UPDATE_RECORD: {
        PageUpdateRecord::constructPageUpdateRecordFromBytes(
            retVal.pageUpdateRecord, bytes, offset);
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
    case PAGE_UPDATE_RECORD: {
        pageUpdateRecord.writePageUpdateLogToBytes(bytes, offset);
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
