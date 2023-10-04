#pragma once

#include "common/exception/not_implemented.h"
#include "common/exception/runtime.h"
#include "common/rel_direction.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "common/utils.h"

namespace kuzu {
namespace storage {

enum class ListType : uint8_t {
    ADJ_LISTS = 0,
    REL_PROPERTY_LISTS = 1,
};

enum class ListFileType : uint8_t {
    BASE_LISTS = 0,
    HEADERS = 1,
    METADATA = 2,
};

enum class ColumnType : uint8_t {
    NODE_PROPERTY_COLUMN = 0,
    ADJ_COLUMN = 1,
    REL_PROPERTY_COLUMN = 2,
};

struct RelNodeTableAndDir {
    common::table_id_t relTableID;
    common::RelDataDirection dir;

    RelNodeTableAndDir() = default;

    RelNodeTableAndDir(common::table_id_t relTableID, common::RelDataDirection dir)
        : relTableID{relTableID}, dir{dir} {}

    inline bool operator==(const RelNodeTableAndDir& rhs) const {
        return relTableID == rhs.relTableID && dir == rhs.dir;
    }
};

struct AdjListsID {
    RelNodeTableAndDir relNodeTableAndDir;

    AdjListsID() = default;

    explicit AdjListsID(RelNodeTableAndDir relNodeTableAndDir)
        : relNodeTableAndDir{relNodeTableAndDir} {}

    inline bool operator==(const AdjListsID& rhs) const {
        return relNodeTableAndDir == rhs.relNodeTableAndDir;
    }
};

struct RelPropertyListsID {
    RelNodeTableAndDir relNodeTableAndDir;
    common::property_id_t propertyID;

    RelPropertyListsID() = default;

    RelPropertyListsID(RelNodeTableAndDir relNodeTableAndDir, common::property_id_t propertyID)
        : relNodeTableAndDir{relNodeTableAndDir}, propertyID{propertyID} {}

    inline bool operator==(const RelPropertyListsID& rhs) const {
        return relNodeTableAndDir == rhs.relNodeTableAndDir && propertyID == rhs.propertyID;
    }
};

struct ListFileID {
    ListType listType;
    ListFileType listFileType;
    union {
        AdjListsID adjListsID;
        RelPropertyListsID relPropertyListID;
    };

    ListFileID() = default;

    ListFileID(ListFileType listFileType, AdjListsID adjListsID)
        : listType{ListType::ADJ_LISTS}, listFileType{listFileType}, adjListsID{adjListsID} {}

    ListFileID(ListFileType listFileType, RelPropertyListsID relPropertyListID)
        : listType{ListType::REL_PROPERTY_LISTS}, listFileType{listFileType},
          relPropertyListID{relPropertyListID} {}

    bool operator==(const ListFileID& rhs) const;
};

struct NodePropertyColumnID {
    common::table_id_t tableID;
    common::property_id_t propertyID;

    NodePropertyColumnID() = default;

    NodePropertyColumnID(common::table_id_t tableID, common::property_id_t propertyID)
        : tableID{tableID}, propertyID{propertyID} {}

    inline bool operator==(const NodePropertyColumnID& rhs) const {
        return tableID == rhs.tableID && propertyID == rhs.propertyID;
    }
};

struct AdjColumnID {
    RelNodeTableAndDir relNodeTableAndDir;

    AdjColumnID() = default;

    explicit AdjColumnID(RelNodeTableAndDir relNodeTableAndDir)
        : relNodeTableAndDir{relNodeTableAndDir} {}

    inline bool operator==(const AdjColumnID& rhs) const {
        return relNodeTableAndDir == rhs.relNodeTableAndDir;
    }
};

struct RelPropertyColumnID {
    RelNodeTableAndDir relNodeTableAndDir;
    common::property_id_t propertyID;

    RelPropertyColumnID() = default;

    RelPropertyColumnID(RelNodeTableAndDir relNodeTableAndDir, common::property_id_t propertyID)
        : relNodeTableAndDir{relNodeTableAndDir}, propertyID{propertyID} {}

    inline bool operator==(const RelPropertyColumnID& rhs) const {
        return relNodeTableAndDir == rhs.relNodeTableAndDir && propertyID == rhs.propertyID;
    }
};

struct ColumnFileID {
    ColumnType columnType;
    union {
        NodePropertyColumnID nodePropertyColumnID;
        AdjColumnID adjColumnID;
        RelPropertyColumnID relPropertyColumnID;
    };

    ColumnFileID() = default;

    explicit ColumnFileID(NodePropertyColumnID nodePropertyColumnID,
        ColumnType columnType = ColumnType::NODE_PROPERTY_COLUMN)
        : columnType{columnType}, nodePropertyColumnID{nodePropertyColumnID} {}

    explicit ColumnFileID(AdjColumnID adjColumnID)
        : columnType{ColumnType::ADJ_COLUMN}, adjColumnID{adjColumnID} {}

    explicit ColumnFileID(RelPropertyColumnID relPropertyColumnID)
        : columnType{ColumnType::REL_PROPERTY_COLUMN}, relPropertyColumnID{relPropertyColumnID} {}

    bool operator==(const ColumnFileID& rhs) const;
};

struct NodeIndexID {
    common::table_id_t tableID;

    NodeIndexID() = default;

    explicit NodeIndexID(common::table_id_t tableID) : tableID{tableID} {}

    inline bool operator==(const NodeIndexID& rhs) const { return tableID == rhs.tableID; }
};

enum class StorageStructureType : uint8_t {
    COLUMN = 0,
    LISTS = 1,
    NODE_INDEX = 2,
    DATA = 3,
    METADATA = 4,
};

std::string storageStructureTypeToString(StorageStructureType storageStructureType);

// StorageStructureIDs start with 1 byte type and 1 byte isOverflow field followed with additional
// bytes needed by the different log types. We don't need these to be byte aligned because they are
// not stored in memory. These are used to serialize and deserialize log entries.
struct StorageStructureID {
    StorageStructureType storageStructureType;
    bool isOverflow;
    bool isNullBits;
    union {
        ColumnFileID columnFileID;
        ListFileID listFileID;
        NodeIndexID nodeIndexID;
    };

    bool operator==(const StorageStructureID& rhs) const;

    static StorageStructureID newDataID();
    static StorageStructureID newMetadataID();

    static StorageStructureID newNodePropertyColumnID(
        common::table_id_t tableID, common::property_id_t propertyID);

    static StorageStructureID newRelPropertyColumnID(common::table_id_t relTableID,
        common::RelDataDirection dir, common::property_id_t propertyID);

    static StorageStructureID newAdjColumnID(
        common::table_id_t relTableID, common::RelDataDirection dir);

    static StorageStructureID newNodeIndexID(common::table_id_t tableID);

    static StorageStructureID newAdjListsID(
        common::table_id_t relTableID, common::RelDataDirection dir, ListFileType listFileType);

    static StorageStructureID newRelPropertyListsID(common::table_id_t relTableID,
        common::RelDataDirection dir, common::property_id_t propertyID, ListFileType listFileType);
};

enum class WALRecordType : uint8_t {
    PAGE_UPDATE_OR_INSERT_RECORD = 1,
    TABLE_STATISTICS_RECORD = 2,
    COMMIT_RECORD = 3,
    CATALOG_RECORD = 4,
    NODE_TABLE_RECORD = 5,
    REL_TABLE_RECORD = 6,
    REL_TABLE_GROUP_RECORD = 7,
    RDF_GRAPH_RECORD = 8,
    // Records the nextBytePosToWriteTo field's last value before the write trx started. This is
    // used when rolling back to restore this value.
    OVERFLOW_FILE_NEXT_BYTE_POS_RECORD = 17,
    COPY_NODE_RECORD = 18,
    COPY_REL_RECORD = 19,
    DROP_TABLE_RECORD = 20,
    DROP_PROPERTY_RECORD = 21,
    ADD_PROPERTY_RECORD = 22,
};

std::string walRecordTypeToString(WALRecordType walRecordType);

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

    explicit CommitRecord(uint64_t transactionID) : transactionID{transactionID} {}

    inline bool operator==(const CommitRecord& rhs) const {
        return transactionID == rhs.transactionID;
    }
};

struct NodeTableRecord {
    common::table_id_t tableID;

    NodeTableRecord() = default;
    explicit NodeTableRecord(common::table_id_t tableID) : tableID{tableID} {}

    inline bool operator==(const NodeTableRecord& rhs) const { return tableID == rhs.tableID; }
};

struct RelTableRecord {
    common::table_id_t tableID;

    RelTableRecord() = default;
    explicit RelTableRecord(common::table_id_t tableID) : tableID{tableID} {}

    inline bool operator==(const RelTableRecord& rhs) const { return tableID == rhs.tableID; }
};

struct RdfGraphRecord {
    common::table_id_t tableID;
    NodeTableRecord nodeTableRecord;
    RelTableRecord relTableRecord;

    RdfGraphRecord() = default;
    RdfGraphRecord(
        common::table_id_t tableID, NodeTableRecord nodeTableRecord, RelTableRecord relTableRecord)
        : tableID{tableID}, nodeTableRecord{nodeTableRecord}, relTableRecord{relTableRecord} {}

    inline bool operator==(const RdfGraphRecord& rhs) const {
        return tableID == rhs.tableID && nodeTableRecord == rhs.nodeTableRecord &&
               relTableRecord == rhs.relTableRecord;
    }
};

struct DiskOverflowFileNextBytePosRecord {
    StorageStructureID storageStructureID;
    uint64_t prevNextBytePosToWriteTo;

    DiskOverflowFileNextBytePosRecord() = default;

    DiskOverflowFileNextBytePosRecord(
        StorageStructureID storageStructureID, uint64_t prevNextByteToWriteTo)
        : storageStructureID{storageStructureID}, prevNextBytePosToWriteTo{prevNextByteToWriteTo} {}

    inline bool operator==(const DiskOverflowFileNextBytePosRecord& rhs) const {
        return storageStructureID == rhs.storageStructureID &&
               prevNextBytePosToWriteTo == rhs.prevNextBytePosToWriteTo;
    }
};

struct CopyNodeRecord {
    common::table_id_t tableID;
    common::page_idx_t startPageIdx;

    CopyNodeRecord() = default;

    explicit CopyNodeRecord(common::table_id_t tableID, common::page_idx_t startPageIdx)
        : tableID{tableID}, startPageIdx{startPageIdx} {}

    inline bool operator==(const CopyNodeRecord& rhs) const {
        return tableID == rhs.tableID && startPageIdx == rhs.startPageIdx;
    }
};

struct CopyRelRecord {
    common::table_id_t tableID;

    CopyRelRecord() = default;

    explicit CopyRelRecord(common::table_id_t tableID) : tableID{tableID} {}

    inline bool operator==(const CopyRelRecord& rhs) const { return tableID == rhs.tableID; }
};

struct TableStatisticsRecord {
    // TODO(Guodong): Better to replace the bool with an enum.
    bool isNodeTable;

    TableStatisticsRecord() = default;

    explicit TableStatisticsRecord(bool isNodeTable) : isNodeTable{isNodeTable} {}

    inline bool operator==(const TableStatisticsRecord& rhs) const {
        return isNodeTable == rhs.isNodeTable;
    }
};

struct DropTableRecord {
    common::table_id_t tableID;

    DropTableRecord() = default;

    explicit DropTableRecord(common::table_id_t tableID) : tableID{tableID} {}

    inline bool operator==(const DropTableRecord& rhs) const { return tableID == rhs.tableID; }
};

struct DropPropertyRecord {
    common::table_id_t tableID;
    common::property_id_t propertyID;

    DropPropertyRecord() = default;

    DropPropertyRecord(common::table_id_t tableID, common::property_id_t propertyID)
        : tableID{tableID}, propertyID{propertyID} {}

    inline bool operator==(const DropPropertyRecord& rhs) const {
        return tableID == rhs.tableID && propertyID == rhs.propertyID;
    }
};

struct AddPropertyRecord {
    common::table_id_t tableID;
    common::property_id_t propertyID;

    AddPropertyRecord() = default;

    AddPropertyRecord(common::table_id_t tableID, common::property_id_t propertyID)
        : tableID{tableID}, propertyID{propertyID} {}

    inline bool operator==(const AddPropertyRecord& rhs) const {
        return tableID == rhs.tableID && propertyID == rhs.propertyID;
    }
};

struct WALRecord {
    WALRecordType recordType;
    union {
        PageUpdateOrInsertRecord pageInsertOrUpdateRecord;
        CommitRecord commitRecord;
        NodeTableRecord nodeTableRecord;
        RelTableRecord relTableRecord;
        RdfGraphRecord rdfGraphRecord;
        DiskOverflowFileNextBytePosRecord diskOverflowFileNextBytePosRecord;
        CopyNodeRecord copyNodeRecord;
        CopyRelRecord copyRelRecord;
        TableStatisticsRecord tableStatisticsRecord;
        DropTableRecord dropTableRecord;
        DropPropertyRecord dropPropertyRecord;
        AddPropertyRecord addPropertyRecord;
    };

    bool operator==(const WALRecord& rhs) const;

    static WALRecord newPageUpdateRecord(StorageStructureID storageStructureID_,
        uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL);
    static WALRecord newPageInsertRecord(StorageStructureID storageStructureID_,
        uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL);
    static WALRecord newCommitRecord(uint64_t transactionID);
    static WALRecord newTableStatisticsRecord(bool isNodeTable);
    static WALRecord newCatalogRecord();
    static WALRecord newNodeTableRecord(common::table_id_t tableID);
    static WALRecord newRelTableRecord(common::table_id_t tableID);
    static WALRecord newRdfGraphRecord(common::table_id_t rdfGraphID,
        common::table_id_t nodeTableID, common::table_id_t relTableID);
    static WALRecord newOverflowFileNextBytePosRecord(
        StorageStructureID storageStructureID_, uint64_t prevNextByteToWriteTo_);
    static WALRecord newCopyNodeRecord(common::table_id_t tableID, common::page_idx_t startPageIdx);
    static WALRecord newCopyRelRecord(common::table_id_t tableID);
    static WALRecord newDropTableRecord(common::table_id_t tableID);
    static WALRecord newDropPropertyRecord(
        common::table_id_t tableID, common::property_id_t propertyID);
    static WALRecord newAddPropertyRecord(
        common::table_id_t tableID, common::property_id_t propertyID);
    static void constructWALRecordFromBytes(WALRecord& retVal, uint8_t* bytes, uint64_t& offset);
    // This functions assumes that the caller ensures there is enough space in the bytes pointer
    // to write the record. This should be checked by calling numBytesToWrite.
    void writeWALRecordToBytes(uint8_t* bytes, uint64_t& offset);

private:
    static WALRecord newPageInsertOrUpdateRecord(StorageStructureID storageStructureID_,
        uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL, bool isInsert);
};

} // namespace storage
} // namespace kuzu
