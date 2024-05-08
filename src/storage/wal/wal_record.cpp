#include "storage/wal/wal_record.h"

#include "catalog/catalog_entry/catalog_entry.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void WALRecord::serialize(common::Serializer& serializer) const {
    serializer.write(type);
}

std::unique_ptr<WALRecord> WALRecord::deserialize(common::Deserializer& deserializer) {
    WALRecordType type;
    deserializer.deserializeValue(type);
    std::unique_ptr<WALRecord> walRecord;
    switch (type) {
    case WALRecordType::CREATE_TABLE_RECORD: {
        walRecord = CreateTableRecord::deserialize(deserializer);
    } break;
    case WALRecordType::DROP_TABLE_RECORD: {
        walRecord = DropTableRecord::deserialize(deserializer);
    } break;
    case WALRecordType::CATALOG_RECORD: {
        walRecord = CatalogRecord::deserialize(deserializer);
    } break;
    case WALRecordType::TABLE_STATISTICS_RECORD: {
        walRecord = TableStatisticsRecord::deserialize(deserializer);
    } break;
    case WALRecordType::COPY_TABLE_RECORD: {
        walRecord = CopyTableRecord::deserialize(deserializer);
    } break;
    case WALRecordType::PAGE_UPDATE_OR_INSERT_RECORD: {
        walRecord = PageUpdateOrInsertRecord::deserialize(deserializer);
    } break;
    case WALRecordType::COMMIT_RECORD: {
        walRecord = CommitRecord::deserialize(deserializer);
    } break;
    case WALRecordType::INVALID_RECORD: {
        throw RuntimeException("Corrupted wal file. Read out invalid WAL record type.");
    }
    default: {
        KU_UNREACHABLE;
    }
    }
    walRecord->type = type;
    return walRecord;
}

void PageUpdateOrInsertRecord::serialize(common::Serializer& serializer) const {
    WALRecord::serialize(serializer);
    serializer.write(dbFileID);
    serializer.write(pageIdxInOriginalFile);
    serializer.write(pageIdxInWAL);
    serializer.write(isInsert);
}

std::unique_ptr<PageUpdateOrInsertRecord> PageUpdateOrInsertRecord::deserialize(
    common::Deserializer& deserializer) {
    auto retVal = std::make_unique<PageUpdateOrInsertRecord>();
    deserializer.deserializeValue(retVal->dbFileID);
    deserializer.deserializeValue(retVal->pageIdxInOriginalFile);
    deserializer.deserializeValue(retVal->pageIdxInWAL);
    deserializer.deserializeValue(retVal->isInsert);
    return retVal;
}

void CommitRecord::serialize(common::Serializer& serializer) const {
    WALRecord::serialize(serializer);
    serializer.write(transactionID);
}

std::unique_ptr<CommitRecord> CommitRecord::deserialize(common::Deserializer& deserializer) {
    auto retVal = std::make_unique<CommitRecord>();
    deserializer.deserializeValue(retVal->transactionID);
    return retVal;
}

CreateTableRecord::CreateTableRecord() = default;

CreateTableRecord::CreateTableRecord(catalog::CatalogEntry* catalogEntry)
    : WALRecord{WALRecordType::CREATE_TABLE_RECORD}, catalogEntry{catalogEntry} {}

void CreateTableRecord::serialize(common::Serializer& serializer) const {
    WALRecord::serialize(serializer);
    catalogEntry->serialize(serializer);
}

std::unique_ptr<CreateTableRecord> CreateTableRecord::deserialize(
    common::Deserializer& deserializer) {
    auto catalogEntry = catalog::CatalogEntry::deserialize(deserializer);
    auto retVal = std::make_unique<CreateTableRecord>();
    retVal->ownedCatalogEntry = std::move(catalogEntry);
    return retVal;
}

void CopyTableRecord::serialize(common::Serializer& serializer) const {
    WALRecord::serialize(serializer);
    serializer.write(tableID);
}

std::unique_ptr<CopyTableRecord> CopyTableRecord::deserialize(common::Deserializer& deserializer) {
    auto retVal = std::make_unique<CopyTableRecord>();
    deserializer.deserializeValue(retVal->tableID);
    return retVal;
}

void CatalogRecord::serialize(common::Serializer& serializer) const {
    WALRecord::serialize(serializer);
}

std::unique_ptr<CatalogRecord> CatalogRecord::deserialize(common::Deserializer&) {
    return std::make_unique<CatalogRecord>();
}

void TableStatisticsRecord::serialize(common::Serializer& serializer) const {
    WALRecord::serialize(serializer);
    serializer.write(tableType);
}

std::unique_ptr<TableStatisticsRecord> TableStatisticsRecord::deserialize(
    common::Deserializer& deserializer) {
    auto retVal = std::make_unique<TableStatisticsRecord>();
    deserializer.deserializeValue(retVal->tableType);
    return retVal;
}

void DropTableRecord::serialize(common::Serializer& serializer) const {
    WALRecord::serialize(serializer);
    serializer.write(tableID);
}

std::unique_ptr<DropTableRecord> DropTableRecord::deserialize(common::Deserializer& deserializer) {
    auto retVal = std::make_unique<DropTableRecord>();
    deserializer.deserializeValue(retVal->tableID);
    return retVal;
}

DBFileID DBFileID::newDataFileID() {
    return DBFileID{DBFileType::DATA};
}

DBFileID DBFileID::newMetadataFileID() {
    return DBFileID{DBFileType::METADATA};
}

DBFileID DBFileID::newPKIndexFileID(common::table_id_t tableID) {
    DBFileID retVal{DBFileType::NODE_INDEX};
    retVal.nodeIndexID = NodeIndexID(tableID);
    return retVal;
}

} // namespace storage
} // namespace kuzu
