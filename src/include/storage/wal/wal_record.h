#pragma once

#include "common/enums/table_type.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"

namespace kuzu {
namespace storage {

struct NodeIndexID {
    common::table_id_t tableID;

    NodeIndexID() = default;

    explicit NodeIndexID(common::table_id_t tableID) : tableID{tableID} {}

    inline bool operator==(const NodeIndexID& rhs) const { return tableID == rhs.tableID; }
};

enum class DBFileType : uint8_t {
    NODE_INDEX = 0,
    DATA = 1,
    METADATA = 2,
};

std::string dbFileTypeToString(DBFileType dbFileType);

// DBFileID start with 1 byte type and 1 byte isOverflow field followed with additional
// bytes needed by the different log types. We don't need these to be byte aligned because they are
// not stored in memory. These are used to serialize and deserialize log entries.
struct DBFileID {
    DBFileType dbFileType;
    bool isOverflow;
    union {
        NodeIndexID nodeIndexID;
    };

    bool operator==(const DBFileID& rhs) const;

    static DBFileID newDataFileID();
    static DBFileID newMetadataFileID();
    static DBFileID newPKIndexFileID(common::table_id_t tableID);
};

enum class WALRecordType : uint8_t {
    PAGE_UPDATE_OR_INSERT_RECORD = 1,
    TABLE_STATISTICS_RECORD = 2,
    COMMIT_RECORD = 3,
    CATALOG_RECORD = 4,
    CREATE_TABLE_RECORD = 6,
    CREATE_REL_TABLE_GROUP_RECORD = 7,
    CREATE_RDF_GRAPH_RECORD = 8,
    // Records the nextBytePosToWriteTo field's last value before the write trx started. This is
    // used when rolling back to restore this value.
    OVERFLOW_FILE_NEXT_BYTE_POS_RECORD = 17,
    COPY_TABLE_RECORD = 19,
    DROP_TABLE_RECORD = 20,
    DROP_PROPERTY_RECORD = 21,
    ADD_PROPERTY_RECORD = 22,
};

std::string walRecordTypeToString(WALRecordType walRecordType);

struct PageUpdateOrInsertRecord {
    DBFileID dbFileID;
    // PageIdx in the file of updated storage structure, identified by the dbFileID field
    uint64_t pageIdxInOriginalFile;
    uint64_t pageIdxInWAL;
    bool isInsert;

    PageUpdateOrInsertRecord() = default;

    PageUpdateOrInsertRecord(
        DBFileID dbFileID, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL, bool isInsert)
        : dbFileID{dbFileID}, pageIdxInOriginalFile{pageIdxInOriginalFile},
          pageIdxInWAL{pageIdxInWAL}, isInsert{isInsert} {}

    inline bool operator==(const PageUpdateOrInsertRecord& rhs) const {
        return dbFileID == rhs.dbFileID && pageIdxInOriginalFile == rhs.pageIdxInOriginalFile &&
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

struct CreateTableRecord {
    common::table_id_t tableID;
    common::TableType tableType;

    CreateTableRecord() = default;
    explicit CreateTableRecord(common::table_id_t tableID, common::TableType tableType)
        : tableID{tableID}, tableType{tableType} {}

    inline bool operator==(const CreateTableRecord& rhs) const {
        return tableID == rhs.tableID && tableType == rhs.tableType;
    }
};

struct RdfGraphRecord {
    common::table_id_t tableID;
    CreateTableRecord resourceTableRecord;
    CreateTableRecord literalTableRecord;
    CreateTableRecord resourceTripleTableRecord;
    CreateTableRecord literalTripleTableRecord;

    RdfGraphRecord() = default;
    RdfGraphRecord(common::table_id_t tableID, CreateTableRecord resourceTableRecord,
        CreateTableRecord literalTableRecord, CreateTableRecord resourceTripleTableRecord,
        CreateTableRecord literalTripleTableRecord)
        : tableID{tableID}, resourceTableRecord{resourceTableRecord},
          literalTableRecord{literalTableRecord},
          resourceTripleTableRecord{resourceTripleTableRecord}, literalTripleTableRecord{
                                                                    literalTripleTableRecord} {}

    inline bool operator==(const RdfGraphRecord& rhs) const {
        return tableID == rhs.tableID && resourceTableRecord == rhs.resourceTableRecord &&
               literalTableRecord == rhs.literalTableRecord &&
               resourceTripleTableRecord == rhs.resourceTripleTableRecord &&
               literalTripleTableRecord == rhs.literalTripleTableRecord;
    }
};

struct DiskOverflowFileNextBytePosRecord {
    DBFileID dbFileID;
    uint64_t prevNextBytePosToWriteTo;

    DiskOverflowFileNextBytePosRecord() = default;

    DiskOverflowFileNextBytePosRecord(DBFileID dbFileID, uint64_t prevNextByteToWriteTo)
        : dbFileID{dbFileID}, prevNextBytePosToWriteTo{prevNextByteToWriteTo} {}

    inline bool operator==(const DiskOverflowFileNextBytePosRecord& rhs) const {
        return dbFileID == rhs.dbFileID && prevNextBytePosToWriteTo == rhs.prevNextBytePosToWriteTo;
    }
};

struct CopyTableRecord {
    common::table_id_t tableID;
    common::TableType tableType;

    CopyTableRecord() = default;

    explicit CopyTableRecord(common::table_id_t tableID, common::TableType tableType)
        : tableID{tableID}, tableType{tableType} {}

    inline bool operator==(const CopyTableRecord& rhs) const {
        return tableID == rhs.tableID && tableType == rhs.tableType;
    }
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
        CreateTableRecord createTableRecord;
        RdfGraphRecord rdfGraphRecord;
        DiskOverflowFileNextBytePosRecord diskOverflowFileNextBytePosRecord;
        CopyTableRecord copyTableRecord;
        TableStatisticsRecord tableStatisticsRecord;
        DropTableRecord dropTableRecord;
        DropPropertyRecord dropPropertyRecord;
        AddPropertyRecord addPropertyRecord;
    };

    bool operator==(const WALRecord& rhs) const;

    static WALRecord newPageUpdateRecord(
        DBFileID dbFileID, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL);
    static WALRecord newPageInsertRecord(
        DBFileID dbFileID, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL);
    static WALRecord newCommitRecord(uint64_t transactionID);
    static WALRecord newTableStatisticsRecord(bool isNodeTable);
    static WALRecord newCatalogRecord();
    static WALRecord newCreateTableRecord(common::table_id_t tableID, common::TableType tableType);
    static WALRecord newRdfGraphRecord(common::table_id_t rdfGraphID,
        common::table_id_t resourceTableID, common::table_id_t literalTableID,
        common::table_id_t resourceTripleTableID, common::table_id_t literalTripleTableID);
    static WALRecord newOverflowFileNextBytePosRecord(
        DBFileID dbFileID, uint64_t prevNextByteToWriteTo_);
    static WALRecord newCopyTableRecord(common::table_id_t tableID, common::TableType tableType);
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
    static WALRecord newPageInsertOrUpdateRecord(
        DBFileID dbFileID, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL, bool isInsert);
};

} // namespace storage
} // namespace kuzu
