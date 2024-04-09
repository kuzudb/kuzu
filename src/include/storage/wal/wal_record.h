#pragma once

#include "common/enums/table_type.h"
#include "common/types/internal_id_t.h"
#include "function/hash/hash_functions.h"

namespace kuzu {
namespace common {
class Serializer;
class Deserializer;
} // namespace common

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

// DBFileID start with 1 byte type and 1 byte isOverflow field followed with additional
// bytes needed by the different log types. We don't need these to be byte aligned because they are
// not stored in memory. These are used to serialize and deserialize log entries.
struct DBFileID {
    DBFileType dbFileType;
    bool isOverflow;
    NodeIndexID nodeIndexID;

    DBFileID() = default;
    explicit DBFileID(DBFileType dbFileType)
        : dbFileType(dbFileType), isOverflow(false), nodeIndexID(common::INVALID_TABLE_ID) {}
    bool operator==(const DBFileID& rhs) const = default;

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
    CREATE_RDF_GRAPH_RECORD = 8,
    COPY_TABLE_RECORD = 19,
    DROP_TABLE_RECORD = 20,
    DROP_PROPERTY_RECORD = 21,
    ADD_PROPERTY_RECORD = 22,
};

struct WALRecord {
    WALRecordType type;

    WALRecord() = default;
    explicit WALRecord(WALRecordType type) : type{type} {}
    virtual ~WALRecord() = default;

    virtual void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<WALRecord> deserialize(common::Deserializer& deserializer);
};

struct PageUpdateOrInsertRecord final : public WALRecord {
    DBFileID dbFileID;
    // PageIdx in the file of updated storage structure, identified by the dbFileID field
    uint64_t pageIdxInOriginalFile;
    uint64_t pageIdxInWAL;
    bool isInsert;

    PageUpdateOrInsertRecord() = default;
    PageUpdateOrInsertRecord(DBFileID dbFileID, uint64_t pageIdxInOriginalFile,
        uint64_t pageIdxInWAL, bool isInsert)
        : WALRecord{WALRecordType::PAGE_UPDATE_OR_INSERT_RECORD}, dbFileID{dbFileID},
          pageIdxInOriginalFile{pageIdxInOriginalFile}, pageIdxInWAL{pageIdxInWAL},
          isInsert{isInsert} {}
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<PageUpdateOrInsertRecord> deserialize(
        common::Deserializer& deserializer);
};

struct CommitRecord final : public WALRecord {
    uint64_t transactionID;

    CommitRecord() = default;
    explicit CommitRecord(uint64_t transactionID)
        : WALRecord{WALRecordType::COMMIT_RECORD}, transactionID{transactionID} {}

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<CommitRecord> deserialize(common::Deserializer& deserializer);
};

struct CreateTableRecord final : public WALRecord {
    common::table_id_t tableID;
    common::TableType tableType;

    CreateTableRecord() = default;
    explicit CreateTableRecord(common::table_id_t tableID, common::TableType tableType)
        : WALRecord{WALRecordType::CREATE_TABLE_RECORD}, tableID{tableID}, tableType{tableType} {}

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<CreateTableRecord> deserialize(common::Deserializer& deserializer);
};

struct CatalogRecord final : public WALRecord {
    CatalogRecord() : WALRecord{WALRecordType::CATALOG_RECORD} {}

    void serialize(common::Serializer& serializer) const override {
        WALRecord::serialize(serializer);
    }
    static std::unique_ptr<CatalogRecord> deserialize(common::Deserializer&) {
        return std::make_unique<CatalogRecord>();
    }
};

struct RdfGraphRecord final : public WALRecord {
    common::table_id_t tableID;
    std::unique_ptr<WALRecord> resourceTableRecord;
    std::unique_ptr<WALRecord> literalTableRecord;
    std::unique_ptr<WALRecord> resourceTripleTableRecord;
    std::unique_ptr<WALRecord> literalTripleTableRecord;

    RdfGraphRecord() = default;
    RdfGraphRecord(common::table_id_t tableID,
        std::unique_ptr<CreateTableRecord> resourceTableRecord,
        std::unique_ptr<CreateTableRecord> literalTableRecord,
        std::unique_ptr<CreateTableRecord> resourceTripleTableRecord,
        std::unique_ptr<CreateTableRecord> literalTripleTableRecord)
        : WALRecord{WALRecordType::CREATE_RDF_GRAPH_RECORD}, tableID{tableID},
          resourceTableRecord{std::move(resourceTableRecord)},
          literalTableRecord{std::move(literalTableRecord)},
          resourceTripleTableRecord{std::move(resourceTripleTableRecord)},
          literalTripleTableRecord{std::move(literalTripleTableRecord)} {}

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<RdfGraphRecord> deserialize(common::Deserializer& deserializer);
};

struct CopyTableRecord final : public WALRecord {
    common::table_id_t tableID;

    CopyTableRecord() = default;
    explicit CopyTableRecord(common::table_id_t tableID)
        : WALRecord{WALRecordType::COPY_TABLE_RECORD}, tableID{tableID} {}

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<CopyTableRecord> deserialize(common::Deserializer& deserializer);
};

struct TableStatisticsRecord final : public WALRecord {
    common::TableType tableType;

    TableStatisticsRecord() = default;
    explicit TableStatisticsRecord(common::TableType tableType)
        : WALRecord{WALRecordType::TABLE_STATISTICS_RECORD}, tableType{tableType} {}

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<TableStatisticsRecord> deserialize(common::Deserializer& deserializer);
};

struct DropTableRecord final : public WALRecord {
    common::table_id_t tableID;

    DropTableRecord() = default;
    explicit DropTableRecord(common::table_id_t tableID)
        : WALRecord{WALRecordType::DROP_TABLE_RECORD}, tableID{tableID} {}

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<DropTableRecord> deserialize(common::Deserializer& deserializer);
};

struct DropPropertyRecord final : public WALRecord {
    common::table_id_t tableID;
    common::property_id_t propertyID;

    DropPropertyRecord() = default;
    DropPropertyRecord(common::table_id_t tableID, common::property_id_t propertyID)
        : WALRecord{WALRecordType::DROP_PROPERTY_RECORD}, tableID{tableID}, propertyID{propertyID} {
    }

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<DropPropertyRecord> deserialize(common::Deserializer& deserializer);
};

struct AddPropertyRecord final : public WALRecord {
    common::table_id_t tableID;
    common::property_id_t propertyID;

    AddPropertyRecord() = default;
    AddPropertyRecord(common::table_id_t tableID, common::property_id_t propertyID)
        : WALRecord{WALRecordType::ADD_PROPERTY_RECORD}, tableID{tableID}, propertyID{propertyID} {}

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<AddPropertyRecord> deserialize(common::Deserializer& deserializer);
};

} // namespace storage
} // namespace kuzu

namespace std {
template<>
struct hash<kuzu::storage::DBFileID> {
    size_t operator()(const kuzu::storage::DBFileID& fileId) const {
        auto dbFileTypeHash = std::hash<uint8_t>()(static_cast<uint8_t>(fileId.dbFileType));
        auto isOverflowHash = std::hash<bool>()(fileId.isOverflow);
        auto nodeIndexIDHash = std::hash<kuzu::common::table_id_t>()(fileId.nodeIndexID.tableID);
        return kuzu::function::combineHashScalar(dbFileTypeHash,
            kuzu::function::combineHashScalar(isOverflowHash, nodeIndexIDHash));
    }
};
} // namespace std
