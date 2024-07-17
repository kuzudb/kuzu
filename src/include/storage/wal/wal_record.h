#pragma once

#include "binder/ddl/bound_alter_info.h"
#include "catalog/catalog_entry/catalog_entry.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "common/types/internal_id_t.h"

namespace kuzu {
namespace common {
class Serializer;
class Deserializer;
} // namespace common

namespace storage {

enum class WALRecordType : uint8_t {
    INVALID_RECORD = 0, // This is not used for any record. 0 is reserved to detect cases where we
                        // accidentally read from an empty buffer.
    COMMIT_RECORD = 2,
    COPY_TABLE_RECORD = 3,
    CREATE_CATALOG_ENTRY_RECORD = 4,
    DROP_CATALOG_ENTRY_RECORD = 10,
    ALTER_TABLE_ENTRY_RECORD = 11,
    UPDATE_SEQUENCE_RECORD = 40,
    CHECKPOINT_RECORD = 50,
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

struct CommitRecord final : public WALRecord {
    uint64_t transactionID;

    CommitRecord() = default;
    explicit CommitRecord(uint64_t transactionID)
        : WALRecord{WALRecordType::COMMIT_RECORD}, transactionID{transactionID} {}

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<CommitRecord> deserialize(common::Deserializer& deserializer);
};

struct CheckpointRecord final : public WALRecord {
    CheckpointRecord() : WALRecord{WALRecordType::CHECKPOINT_RECORD} {}

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<CheckpointRecord> deserialize(common::Deserializer& deserializer);
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
