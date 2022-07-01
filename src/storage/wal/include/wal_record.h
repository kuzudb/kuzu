#pragma once

#include "src/common/include/utils.h"
#include "src/common/types/include/types_include.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

struct StructuredNodePropertyColumnID {
    label_t nodeLabel;
    uint32_t propertyID;

    inline bool operator==(const StructuredNodePropertyColumnID& rhs) const {
        return nodeLabel == rhs.nodeLabel && propertyID == rhs.propertyID;
    }
};

enum StorageStructureType : uint8_t {
    STRUCTURED_NODE_PROPERTY_COLUMN = 0,
};

// StorageStructureIDs start with 1 byte type and 1 byte isOverflow field followed with additional
// bytes needed by the different log types. We don't need these to be byte aligned because they are
// not stored in memory. These are used to serialize and deserialize log entries.
struct StorageStructureID {
    StorageStructureType storageStructureType;
    bool isOverflow;
    union {
        StructuredNodePropertyColumnID structuredNodePropertyColumnID;
    };

    inline bool operator==(const StorageStructureID& rhs) const {
        if (storageStructureType != rhs.storageStructureType || isOverflow != rhs.isOverflow) {
            return false;
        }
        switch (storageStructureType) {
        case STRUCTURED_NODE_PROPERTY_COLUMN: {
            return structuredNodePropertyColumnID == rhs.structuredNodePropertyColumnID;
        }
        default: {
            throw RuntimeException(
                "Unrecognized StorageStructureType inside ==. StorageStructureType:" +
                to_string(storageStructureType));
        }
        }
    }

    inline static StorageStructureID newStructuredNodePropertyMainColumnID(
        label_t nodeLabel, uint32_t propertyID) {
        return newStructuredNodePropertyColumnID(nodeLabel, propertyID, false /* is main file */);
    }

    inline static StorageStructureID newStructuredNodePropertyColumnOverflowPagesID(
        label_t nodeLabel, uint32_t propertyID) {
        return newStructuredNodePropertyColumnID(
            nodeLabel, propertyID, true /* is overflow file */);
    }

private:
    static StorageStructureID newStructuredNodePropertyColumnID(
        label_t nodeLabel, uint32_t propertyID, bool isOverflow);
};

enum WALRecordType : uint8_t {
    PAGE_UPDATE_OR_INSERT_RECORD = 0,
    NODES_METADATA_RECORD = 1,
    COMMIT_RECORD = 2,
};

struct PageUpdateOrInsertRecord {
    StorageStructureID storageStructureID;
    // PageIdx in the file of updated storage structure, identified by the storageStructureID field
    uint64_t pageIdxInOriginalFile;
    uint64_t pageIdxInWAL;
    bool isInsert;

    inline bool operator==(const PageUpdateOrInsertRecord& rhs) const {
        return storageStructureID == rhs.storageStructureID &&
               pageIdxInOriginalFile == rhs.pageIdxInOriginalFile &&
               pageIdxInWAL == rhs.pageIdxInWAL && isInsert == rhs.isInsert;
    }

    static PageUpdateOrInsertRecord newPageInsertOrUpdateRecord(
        StorageStructureID storageStructureID_, uint64_t pageIdxInOriginalFile,
        uint64_t pageIdxInWAL, bool isInsert);
};

struct CommitRecord {
    uint64_t transactionID;

    inline bool operator==(const CommitRecord& rhs) const {
        return transactionID == rhs.transactionID;
    }

    static CommitRecord newCommitRecord(uint64_t transactionID);
};

struct WALRecord {
    WALRecordType recordType;
    union {
        PageUpdateOrInsertRecord pageInsertOrUpdateRecord;
        CommitRecord commitRecord;
    };

    bool operator==(const WALRecord& rhs) const {
        if (recordType != rhs.recordType) {
            return false;
        }
        switch (recordType) {
        case PAGE_UPDATE_OR_INSERT_RECORD: {
            return pageInsertOrUpdateRecord == rhs.pageInsertOrUpdateRecord;
        }
        case COMMIT_RECORD: {
            return commitRecord == rhs.commitRecord;
        }
        case NODES_METADATA_RECORD: {
            // NodesMetadataRecords are empty so are always equal
            return true;
        }
        default: {
            throw RuntimeException(
                "Unrecognized WAL record type inside ==. recordType: " + to_string(recordType));
        }
        }
    }

    static WALRecord newPageUpdateRecord(StorageStructureID storageStructureID_,
        uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL);
    static WALRecord newPageInsertRecord(StorageStructureID storageStructureID_,
        uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL);
    static WALRecord newCommitRecord(uint64_t transactionID);
    static WALRecord newNodeMetadataRecord();
    static void constructWALRecordFromBytes(WALRecord& retVal, uint8_t* bytes, uint64_t& offset);
    // This functions assumes that the caller ensures there is enough space in the bytes pointer
    // to write the record. This should be checked by calling numBytesToWrite.
    void writeWALRecordToBytes(uint8_t* bytes, uint64_t& offset);

private:
    static WALRecord newPageInsertOrUpdateRecord(StorageStructureID storageStructureID_,
        uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL, bool isInsert);
};

} // namespace storage
} // namespace graphflow
