#pragma once

#include "binder/ddl/bound_alter_info.h"
#include "catalog/catalog_entry/catalog_entry.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
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
    INVALID_RECORD = 0, // This is not used for any record. 0 is reserved to detect cases where we
                        // accidentally read from an empty buffer.
    CATALOG_RECORD = 1,
    COMMIT_RECORD = 2,
    COPY_TABLE_RECORD = 3,
    CREATE_CATALOG_ENTRY_RECORD = 4,
    DROP_CATALOG_ENTRY_RECORD = 10,
    ALTER_TABLE_ENTRY_RECORD = 11,
    PAGE_UPDATE_OR_INSERT_RECORD = 20,
    TABLE_STATISTICS_RECORD = 30,
    UPDATE_SEQUENCE_RECORD = 40,
};

struct WALRecord {
    WALRecordType type;

    WALRecord() = default;
    explicit WALRecord(WALRecordType type) : type{type} {}
    virtual ~WALRecord() = default;

    virtual void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<WALRecord> deserialize(common::Deserializer& deserializer);

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const WALRecord&, const TARGET&>(*this);
    }
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

struct CreateCatalogEntryRecord final : public WALRecord {
    catalog::CatalogEntry* catalogEntry;
    std::unique_ptr<catalog::CatalogEntry> ownedCatalogEntry;

    CreateCatalogEntryRecord();
    explicit CreateCatalogEntryRecord(catalog::CatalogEntry* catalogEntry);

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<CreateCatalogEntryRecord> deserialize(
        common::Deserializer& deserializer);
};

struct CopyTableRecord final : public WALRecord {
    common::table_id_t tableID;

    CopyTableRecord() = default;
    explicit CopyTableRecord(common::table_id_t tableID)
        : WALRecord{WALRecordType::COPY_TABLE_RECORD}, tableID{tableID} {}

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<CopyTableRecord> deserialize(common::Deserializer& deserializer);
};

struct CatalogRecord final : public WALRecord {
    CatalogRecord() : WALRecord{WALRecordType::CATALOG_RECORD} {}

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<CatalogRecord> deserialize(common::Deserializer& deserializer);
};

struct TableStatisticsRecord final : public WALRecord {
    common::TableType tableType;

    TableStatisticsRecord() = default;
    explicit TableStatisticsRecord(common::TableType tableType)
        : WALRecord{WALRecordType::TABLE_STATISTICS_RECORD}, tableType{tableType} {}

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<TableStatisticsRecord> deserialize(common::Deserializer& deserializer);
};

struct DropCatalogEntryRecord final : public WALRecord {
    common::table_id_t entryID;
    catalog::CatalogEntryType entryType;

    DropCatalogEntryRecord() = default;
    DropCatalogEntryRecord(common::table_id_t entryID, catalog::CatalogEntryType entryType)
        : WALRecord{WALRecordType::DROP_CATALOG_ENTRY_RECORD}, entryID{entryID},
          entryType{entryType} {}

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<DropCatalogEntryRecord> deserialize(common::Deserializer& deserializer);
};

struct AlterTableEntryRecord final : public WALRecord {
    binder::BoundAlterInfo* alterInfo;
    std::unique_ptr<binder::BoundAlterInfo> ownedAlterInfo;

    AlterTableEntryRecord() = default;
    explicit AlterTableEntryRecord(binder::BoundAlterInfo* alterInfo)
        : WALRecord{WALRecordType::ALTER_TABLE_ENTRY_RECORD}, alterInfo{alterInfo} {}

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<AlterTableEntryRecord> deserialize(common::Deserializer& deserializer);
};
struct UpdateSequenceRecord final : public WALRecord {
    common::sequence_id_t sequenceID;
    catalog::SequenceChangeData data;

    UpdateSequenceRecord() = default;
    UpdateSequenceRecord(common::sequence_id_t sequenceID, catalog::SequenceChangeData data)
        : WALRecord{WALRecordType::UPDATE_SEQUENCE_RECORD}, sequenceID{sequenceID},
          data{std::move(data)} {}

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<UpdateSequenceRecord> deserialize(common::Deserializer& deserializer);
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
