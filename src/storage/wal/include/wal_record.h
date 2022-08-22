#pragma once

#include "src/common/include/utils.h"
#include "src/common/types/include/types_include.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {
enum ListType : uint8_t {
    UNSTRUCTURED_NODE_PROPERTY_LISTS = 0,
    ADJ_LISTS = 1,
    REL_PROPERTY_LISTS = 2,
};

enum ListFileType : uint8_t {
    BASE_LISTS = 0,
    HEADERS = 1,
    METADATA = 2,
};

struct UnstructuredNodePropertyListsID {
    label_t nodeLabel;
    UnstructuredNodePropertyListsID() = default;

    UnstructuredNodePropertyListsID(label_t nodeLabel) : nodeLabel{nodeLabel} {}

    inline bool operator==(const UnstructuredNodePropertyListsID& rhs) const {
        return nodeLabel == rhs.nodeLabel;
    }
};

struct RelNodeLabelAndDir {
    label_t relLabel;
    label_t srcNodeLabel;
    RelDirection dir;

    RelNodeLabelAndDir() = default;

    RelNodeLabelAndDir(label_t relLabel, label_t srcNodeLabel, RelDirection dir)
        : relLabel{relLabel}, srcNodeLabel{srcNodeLabel}, dir{dir} {}

    inline bool operator==(const RelNodeLabelAndDir& rhs) const {
        return relLabel == rhs.relLabel && srcNodeLabel == rhs.srcNodeLabel && dir == rhs.dir;
    }
};

struct AdjListsID {
    RelNodeLabelAndDir relLabelAndDir;

    AdjListsID() = default;

    AdjListsID(RelNodeLabelAndDir relLabelAndDir) : relLabelAndDir{relLabelAndDir} {}

    inline bool operator==(const AdjListsID& rhs) const {
        return relLabelAndDir == rhs.relLabelAndDir;
    }
};

struct RelPropertyListID {
    RelNodeLabelAndDir relLabelAndDir;
    uint32_t propertyID;

    RelPropertyListID() = default;

    RelPropertyListID(RelNodeLabelAndDir relLabelAndDir, uint32_t propertyID)
        : relLabelAndDir{relLabelAndDir}, propertyID{propertyID} {}

    inline bool operator==(const RelPropertyListID& rhs) const {
        return relLabelAndDir == rhs.relLabelAndDir && propertyID == rhs.propertyID;
    }
};

struct ListFileID {
    ListType listType;
    ListFileType listFileType;
    union {
        UnstructuredNodePropertyListsID unstructuredNodePropertyListsID;
        AdjListsID adjListsID;
        RelPropertyListID relPropertyListID;
    };

    ListFileID() = default;

    ListFileID(
        ListFileType listFileType, UnstructuredNodePropertyListsID unstructuredNodePropertyListsID)
        : listType{UNSTRUCTURED_NODE_PROPERTY_LISTS}, listFileType{listFileType},
          unstructuredNodePropertyListsID{unstructuredNodePropertyListsID} {}

    ListFileID(ListFileType listFileType, AdjListsID adjListsID)
        : listType{ADJ_LISTS}, listFileType{listFileType}, adjListsID{adjListsID} {}

    ListFileID(ListFileType listFileType, RelPropertyListID relPropertyListID)
        : listType{REL_PROPERTY_LISTS}, listFileType{listFileType}, relPropertyListID{
                                                                        relPropertyListID} {}

    inline bool operator==(const ListFileID& rhs) const {
        if (listType != rhs.listType || listFileType != rhs.listFileType) {
            return false;
        }
        switch (listType) {
        case UNSTRUCTURED_NODE_PROPERTY_LISTS: {
            return unstructuredNodePropertyListsID == rhs.unstructuredNodePropertyListsID;
        }
        case ADJ_LISTS: {
            return adjListsID == rhs.adjListsID;
        }
        case REL_PROPERTY_LISTS: {
            return relPropertyListID == rhs.relPropertyListID;
        }
        default: {
            throw RuntimeException(
                "Unrecognized ListType inside ==. ListType:" + to_string(listType));
        }
        }
    }
};

struct NodeIndexID {
    label_t nodeLabel;

    NodeIndexID() = default;

    NodeIndexID(label_t nodeLabel) : nodeLabel{nodeLabel} {}

    inline bool operator==(const NodeIndexID& rhs) const { return nodeLabel == rhs.nodeLabel; }
};

struct StructuredNodePropertyColumnID {
    label_t nodeLabel;
    uint32_t propertyID;

    StructuredNodePropertyColumnID() = default;

    StructuredNodePropertyColumnID(label_t nodeLabel, uint32_t propertyID)
        : nodeLabel{nodeLabel}, propertyID{propertyID} {}

    inline bool operator==(const StructuredNodePropertyColumnID& rhs) const {
        return nodeLabel == rhs.nodeLabel && propertyID == rhs.propertyID;
    }
};

enum StorageStructureType : uint8_t {
    STRUCTURED_NODE_PROPERTY_COLUMN = 0,
    LISTS = 1,
    NODE_INDEX = 2,
};

// StorageStructureIDs start with 1 byte type and 1 byte isOverflow field followed with additional
// bytes needed by the different log types. We don't need these to be byte aligned because they are
// not stored in memory. These are used to serialize and deserialize log entries.
struct StorageStructureID {
    StorageStructureType storageStructureType;
    bool isOverflow;
    union {
        StructuredNodePropertyColumnID structuredNodePropertyColumnID;
        ListFileID listFileID;
        NodeIndexID nodeIndexID;
    };

    inline bool operator==(const StorageStructureID& rhs) const {
        if (storageStructureType != rhs.storageStructureType || isOverflow != rhs.isOverflow) {
            return false;
        }
        switch (storageStructureType) {
        case STRUCTURED_NODE_PROPERTY_COLUMN: {
            return structuredNodePropertyColumnID == rhs.structuredNodePropertyColumnID;
        }
        case LISTS: {
            return listFileID == rhs.listFileID;
        }
        case NODE_INDEX: {
            return nodeIndexID == rhs.nodeIndexID;
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
    static StorageStructureID newNodeIndexID(label_t nodeLabel);

    static StorageStructureID newUnstructuredNodePropertyListsID(
        label_t nodeLabel, ListFileType listFileType);

    static StorageStructureID newAdjListsID(
        label_t relLabel, label_t srcNodeLabel, RelDirection dir, ListFileType listFileType);

    static StorageStructureID newRelPropertyListsID(label_t relLabel, label_t srcNodeLabel,
        RelDirection dir, uint32_t propertyID, ListFileType listFileType);

private:
    static StorageStructureID newStructuredNodePropertyColumnID(
        label_t nodeLabel, uint32_t propertyID, bool isOverflow);
};

enum WALRecordType : uint8_t {
    PAGE_UPDATE_OR_INSERT_RECORD = 0,
    NODES_METADATA_RECORD = 1,
    COMMIT_RECORD = 2,
    CATALOG_RECORD = 3,
    NODE_TABLE_RECORD = 4,
    REL_TABLE_RECORD = 5,
    // Records the nextBytePosToWriteTo field's last value before the write trx started. This is
    // used when rolling back to restore this value.
    OVERFLOW_FILE_NEXT_BYTE_POS_RECORD = 6,
    COPY_NODE_CSV_RECORD = 7,
    COPY_REL_CSV_RECORD = 8,
};

struct PageUpdateOrInsertRecord {
    StorageStructureID storageStructureID;
    // PageIdx in the file of updated storage structure, identified by the storageStructureID field
    uint64_t pageIdxInOriginalFile;
    uint64_t pageIdxInWAL;
    bool isInsert;

    PageUpdateOrInsertRecord() = default;

    PageUpdateOrInsertRecord(StorageStructureID storageStructureID, uint64_t pageIdxInOriginalFile,
        uint64_t pageIdxInWAL, bool isInsert)
        : storageStructureID{storageStructureID}, pageIdxInOriginalFile{pageIdxInOriginalFile},
          pageIdxInWAL{pageIdxInWAL}, isInsert{isInsert} {}

    inline bool operator==(const PageUpdateOrInsertRecord& rhs) const {
        return storageStructureID == rhs.storageStructureID &&
               pageIdxInOriginalFile == rhs.pageIdxInOriginalFile &&
               pageIdxInWAL == rhs.pageIdxInWAL && isInsert == rhs.isInsert;
    }
};

struct CommitRecord {
    uint64_t transactionID;

    CommitRecord() = default;

    CommitRecord(uint64_t transactionID) : transactionID{transactionID} {}

    inline bool operator==(const CommitRecord& rhs) const {
        return transactionID == rhs.transactionID;
    }
};

struct NodeTableRecord {
    label_t labelID;

    NodeTableRecord() = default;

    NodeTableRecord(label_t labelID) : labelID{labelID} {}

    inline bool operator==(const NodeTableRecord& rhs) const { return labelID == rhs.labelID; }
};

struct RelTableRecord {
    label_t labelID;

    RelTableRecord() = default;

    RelTableRecord(label_t labelID) : labelID{labelID} {}

    inline bool operator==(const RelTableRecord& rhs) const { return labelID == rhs.labelID; }
};

struct OverflowFileNextBytePosRecord {
    StorageStructureID storageStructureID;
    uint64_t prevNextBytePosToWriteTo;

    OverflowFileNextBytePosRecord() = default;

    OverflowFileNextBytePosRecord(
        StorageStructureID storageStructureID, uint64_t prevNextByteToWriteTo)
        : storageStructureID{storageStructureID}, prevNextBytePosToWriteTo{prevNextByteToWriteTo} {}

    inline bool operator==(const OverflowFileNextBytePosRecord& rhs) const {
        return storageStructureID == rhs.storageStructureID &&
               prevNextBytePosToWriteTo == rhs.prevNextBytePosToWriteTo;
    }
};

struct CopyNodeCSVRecord {
    label_t labelID;

    CopyNodeCSVRecord() = default;

    CopyNodeCSVRecord(label_t labelID) : labelID{labelID} {}

    inline bool operator==(const CopyNodeCSVRecord& rhs) const { return labelID == rhs.labelID; }
};

struct CopyRelCSVRecord {
    label_t labelID;

    CopyRelCSVRecord() = default;

    CopyRelCSVRecord(label_t labelID) : labelID{labelID} {}

    inline bool operator==(const CopyRelCSVRecord& rhs) const { return labelID == rhs.labelID; }
};

struct WALRecord {
    WALRecordType recordType;
    union {
        PageUpdateOrInsertRecord pageInsertOrUpdateRecord;
        CommitRecord commitRecord;
        NodeTableRecord nodeTableRecord;
        RelTableRecord relTableRecord;
        OverflowFileNextBytePosRecord overflowFileNextBytePosRecord;
        CopyNodeCSVRecord copyNodeCsvRecord;
        CopyRelCSVRecord copyRelCsvRecord;
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
        case CATALOG_RECORD: {
            // CatalogRecords are empty so are always equal
            return true;
        }
        case NODE_TABLE_RECORD: {
            return nodeTableRecord == rhs.nodeTableRecord;
        }
        case REL_TABLE_RECORD: {
            return relTableRecord == rhs.relTableRecord;
        }
        case OVERFLOW_FILE_NEXT_BYTE_POS_RECORD: {
            return overflowFileNextBytePosRecord == rhs.overflowFileNextBytePosRecord;
        }
        case COPY_NODE_CSV_RECORD: {
            return copyNodeCsvRecord == rhs.copyNodeCsvRecord;
        }
        case COPY_REL_CSV_RECORD: {
            return copyRelCsvRecord == rhs.copyRelCsvRecord;
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
    static WALRecord newCatalogRecord();
    static WALRecord newNodeTableRecord(label_t labelID);
    static WALRecord newRelTableRecord(label_t labelID);
    static WALRecord newOverflowFileNextBytePosRecord(
        StorageStructureID storageStructureID_, uint64_t prevNextByteToWriteTo_);
    static WALRecord newCopyNodeCSVRecord(label_t labelID);
    static WALRecord newCopyRelCSVRecord(label_t labelID);
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
