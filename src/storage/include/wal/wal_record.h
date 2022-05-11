#pragma once

#include "spdlog/spdlog.h"

#include "src/common/types/include/types_include.h"
#include "src/storage/include/file_handle.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

struct StructuredNodePropertyFileID {
    label_t nodeLabel;
    uint32_t propertyID;

    inline bool operator==(const StructuredNodePropertyFileID& rhs) const {
        return nodeLabel == rhs.nodeLabel && propertyID == rhs.propertyID;
    }

    inline uint64_t numBytesToWrite() { return sizeof(label_t) + sizeof(uint32_t); }

    static void constructStructuredNodePropertyFileIDFromBytes(
        StructuredNodePropertyFileID& retVal, uint8_t* bytes, uint64_t& offset);
    void writeStructuredNodePropertyFileIDToBytes(uint8_t* bytes, uint64_t& offset);
};

struct AdjColumnPropertyFileID {
    label_t nodeLabel;
    label_t relLabel;
    uint32_t propertyID;

    inline bool operator==(const AdjColumnPropertyFileID& rhs) const {
        return nodeLabel == rhs.nodeLabel && relLabel == rhs.relLabel &&
               propertyID == rhs.propertyID;
    }

    inline uint64_t numBytesToWrite() {
        return sizeof(label_t) + sizeof(label_t) + sizeof(uint32_t);
    }

    static void constructAdjColumnPropertyFileIDFromBytes(
        AdjColumnPropertyFileID& retVal, uint8_t* bytes, uint64_t& offset);
    void writeAdjColumnPropertyFileIDToBytes(uint8_t* bytes, uint64_t& offset);
};

enum FileIDType : uint8_t { STRUCTURED_NODE_PROPERTY_FILE_ID = 0, ADJ_COLUMN_PROPERTY_FILE_ID = 1 };

// FileIDs start with 1 byte type and follow with additional bytes needed by the different log
// types. We don't need these to be byte aligned because they are not stored in memory. These are
// used to serialize and deserialize log entries.
struct FileID {
    FileIDType fileIDType;
    union {
        StructuredNodePropertyFileID structuredNodePropFileID;
        AdjColumnPropertyFileID adjColumnPropertyFileID;
    };

    inline bool operator==(const FileID& rhs) const {

        if (fileIDType != rhs.fileIDType) {
            return false;
        }
        switch (fileIDType) {
        case STRUCTURED_NODE_PROPERTY_FILE_ID: {
            return structuredNodePropFileID == rhs.structuredNodePropFileID;
        }
        case ADJ_COLUMN_PROPERTY_FILE_ID: {
            return adjColumnPropertyFileID == rhs.adjColumnPropertyFileID;
        }
        default: {
            throw RuntimeException(
                "Unrecognized FileIDType inside ==. FileIDType:" + to_string(fileIDType));
        }
        }
    }

    inline uint64_t numBytesToWrite() {
        switch (fileIDType) {
        case STRUCTURED_NODE_PROPERTY_FILE_ID: {
            return 1 + structuredNodePropFileID.numBytesToWrite();
        }
        case ADJ_COLUMN_PROPERTY_FILE_ID: {
            return 1 + adjColumnPropertyFileID.numBytesToWrite();
        }
        default: {
            throw RuntimeException("Unrecognized FileIDType inside numBytesToWrite(). FileIDType:" +
                                   to_string(fileIDType));
        }
        }
    }

    static FileID newStructuredNodePropertyFileID(label_t nodeLabel, uint32_t propertyID);
    static FileID newStructuredAdjColumnPropertyFileID(
        label_t nodeLabel, label_t relLabel, uint32_t propertyID);
    static void constructFileIDFromBytes(FileID& retVal, uint8_t* bytes, uint64_t& offset);
    void writeFileIDToBytes(uint8_t* bytes, uint64_t& offset);
};

enum WALRecordType : uint8_t { PAGE_UPDATE_RECORD = 0, COMMIT_RECORD = 1 };

struct PageUpdateRecord {
    FileID fileID;
    uint64_t pageIdxInOriginalFile;
    uint64_t pageIdxInWAL;

    inline bool operator==(const PageUpdateRecord& rhs) const {
        return fileID == rhs.fileID && pageIdxInOriginalFile == rhs.pageIdxInOriginalFile &&
               pageIdxInWAL == rhs.pageIdxInWAL;
    }

    inline uint64_t numBytesToWrite() {
        return fileID.numBytesToWrite() + sizeof(uint64_t) + sizeof(uint64_t);
    }

    static PageUpdateRecord newStructuredNodePropertyPageUpdateRecord(label_t nodeLabel,
        uint32_t propertyID, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL);
    static PageUpdateRecord newStructuredAdjColumnPropertyPageUpdateRecord(label_t nodeLabel,
        label_t relLabel, uint32_t propertyID, uint64_t pageIdxInOriginalFile,
        uint64_t pageIdxInWAL);
    static void constructPageUpdateRecordFromBytes(
        PageUpdateRecord& retVal, uint8_t* bytes, uint64_t& offset);
    void writePageUpdateLogToBytes(uint8_t* bytes, uint64_t& offset);
};

struct CommitRecord {
    uint64_t transactionID;

    inline bool operator==(const CommitRecord& rhs) const {
        return transactionID == rhs.transactionID;
    }

    inline uint64_t numBytesToWrite() { return sizeof(uint64_t); }

    static CommitRecord newCommitRecord(uint64_t transactionID);
    static void constructCommitRecordFromBytes(
        CommitRecord& retVal, uint8_t* bytes, uint64_t& offset);
    void writeCommitRecordToBytes(uint8_t* bytes, uint64_t& offset);
};

struct WALRecord {
    WALRecordType recordType;
    union {
        PageUpdateRecord pageUpdateRecord;
        CommitRecord commitRecord;
    };

    bool operator==(const WALRecord& rhs) const {
        if (recordType != rhs.recordType) {
            return false;
        }
        switch (recordType) {
        case PAGE_UPDATE_RECORD: {
            return pageUpdateRecord == rhs.pageUpdateRecord;
        }
        case COMMIT_RECORD: {
            return commitRecord == rhs.commitRecord;
        }
        default: {
            throw RuntimeException(
                "Unrecognized WAL record type inside ==. recordType: " + to_string(recordType));
        }
        }
    }

    inline uint64_t numBytesToWrite() {
        switch (recordType) {
        case PAGE_UPDATE_RECORD: {
            return 1 + pageUpdateRecord.numBytesToWrite();
        }
        case COMMIT_RECORD: {
            return 1 + commitRecord.numBytesToWrite();
        }
        default: {
            throw RuntimeException(
                "Unrecognized WAL record type inside numBytesToWrite(). recordType: " +
                to_string(recordType));
        }
        }
    }

    static WALRecord newStructuredNodePropertyPageUpdateRecord(label_t nodeLabel,
        uint32_t propertyID, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL);
    static WALRecord newStructuredAdjColumnPropertyPageUpdateRecord(label_t nodeLabel,
        label_t relLabel, uint32_t propertyID, uint64_t pageIdxInOriginalFile,
        uint64_t pageIdxInWAL);
    static WALRecord newCommitRecord(uint64_t transactionID);
    static void constructWALRecordFromBytes(WALRecord& retVal, uint8_t* bytes, uint64_t& offset);
    // This functions assumes that the caller ensures there is enough space in the bytes pointer
    // to write the record. This should be checked by calling numBytesToWrite.
    void writeWALRecordToBytes(uint8_t* bytes, uint64_t& offset);
};

} // namespace storage
} // namespace graphflow
