#include "storage/wal/wal_record.h"

#include "catalog/catalog_entry/catalog_entry.h"
#include "common/exception/runtime.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "main/client_context.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace storage {

void WALRecord::serialize(Serializer& serializer) const {
    serializer.writeDebuggingInfo("type");
    serializer.write(type);
}

std::unique_ptr<WALRecord> WALRecord::deserialize(Deserializer& deserializer,
    const main::ClientContext& clientContext) {
    std::string key;
    auto type = WALRecordType::INVALID_RECORD;
    deserializer.validateDebuggingInfo(key, "type");
    deserializer.deserializeValue(type);
    std::unique_ptr<WALRecord> walRecord;
    switch (type) {
    case WALRecordType::BEGIN_TRANSACTION_RECORD: {
        walRecord = BeginTransactionRecord::deserialize(deserializer);
    } break;
    case WALRecordType::COMMIT_RECORD: {
        walRecord = CommitRecord::deserialize(deserializer);
    } break;
    case WALRecordType::ROLLBACK_RECORD: {
        walRecord = RollbackRecord::deserialize(deserializer);
    } break;
    case WALRecordType::CREATE_CATALOG_ENTRY_RECORD: {
        walRecord = CreateCatalogEntryRecord::deserialize(deserializer);
    } break;
    case WALRecordType::DROP_CATALOG_ENTRY_RECORD: {
        walRecord = DropCatalogEntryRecord::deserialize(deserializer);
    } break;
    case WALRecordType::ALTER_TABLE_ENTRY_RECORD: {
        walRecord = AlterTableEntryRecord::deserialize(deserializer);
    } break;
    case WALRecordType::TABLE_INSERTION_RECORD: {
        walRecord = TableInsertionRecord::deserialize(deserializer, clientContext);
    } break;
    case WALRecordType::NODE_DELETION_RECORD: {
        walRecord = NodeDeletionRecord::deserialize(deserializer, clientContext);
    } break;
    case WALRecordType::NODE_UDPATE_RECORD: {
        walRecord = NodeUpdateRecord::deserialize(deserializer, clientContext);
    } break;
    case WALRecordType::REL_DELETION_RECORD: {
        walRecord = RelDeletionRecord::deserialize(deserializer, clientContext);
    } break;
    case WALRecordType::REL_DETACH_DELETE_RECORD: {
        walRecord = RelDetachDeleteRecord::deserialize(deserializer, clientContext);
    } break;
    case WALRecordType::REL_UPDATE_RECORD: {
        walRecord = RelUpdateRecord::deserialize(deserializer, clientContext);
    } break;
    case WALRecordType::COPY_TABLE_RECORD: {
        walRecord = CopyTableRecord::deserialize(deserializer);
    } break;
    case WALRecordType::CHECKPOINT_RECORD: {
        walRecord = CheckpointRecord::deserialize(deserializer);
    } break;
    case WALRecordType::UPDATE_SEQUENCE_RECORD: {
        walRecord = UpdateSequenceRecord::deserialize(deserializer);
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

void BeginTransactionRecord::serialize(Serializer& serializer) const {
    WALRecord::serialize(serializer);
}

std::unique_ptr<BeginTransactionRecord> BeginTransactionRecord::deserialize(Deserializer&) {
    return std::make_unique<BeginTransactionRecord>();
}

void CommitRecord::serialize(Serializer& serializer) const {
    WALRecord::serialize(serializer);
}

std::unique_ptr<CommitRecord> CommitRecord::deserialize(Deserializer&) {
    return std::make_unique<CommitRecord>();
}

void RollbackRecord::serialize(Serializer& serializer) const {
    WALRecord::serialize(serializer);
}

std::unique_ptr<RollbackRecord> RollbackRecord::deserialize(Deserializer&) {
    return std::make_unique<RollbackRecord>();
}

void CheckpointRecord::serialize(Serializer& serializer) const {
    WALRecord::serialize(serializer);
}

std::unique_ptr<CheckpointRecord> CheckpointRecord::deserialize(Deserializer&) {
    return std::make_unique<CheckpointRecord>();
}

void CreateCatalogEntryRecord::serialize(Serializer& serializer) const {
    WALRecord::serialize(serializer);
    catalogEntry->serialize(serializer);
    idx_t vectorSize = childrenEntries.size();
    serializer.serializeValue(vectorSize);
    for (auto& entry : childrenEntries) {
        entry->serialize(serializer);
    }
    serializer.serializeValue(isInternal);
}

std::unique_ptr<CreateCatalogEntryRecord> CreateCatalogEntryRecord::deserialize(
    Deserializer& deserializer) {
    auto retVal = std::make_unique<CreateCatalogEntryRecord>();
    retVal->ownedCatalogEntry = catalog::CatalogEntry::deserialize(deserializer);
    idx_t vectorSize = 0;
    deserializer.deserializeValue(vectorSize);
    for (auto i = 0u; i < vectorSize; ++i) {
        retVal->ownedChildrenEntries.push_back(catalog::CatalogEntry::deserialize(deserializer));
    }
    bool isInternal = false;
    deserializer.deserializeValue(isInternal);
    retVal->isInternal = isInternal;
    return retVal;
}

void CopyTableRecord::serialize(Serializer& serializer) const {
    WALRecord::serialize(serializer);
    serializer.write(tableID);
}

std::unique_ptr<CopyTableRecord> CopyTableRecord::deserialize(Deserializer& deserializer) {
    auto retVal = std::make_unique<CopyTableRecord>();
    deserializer.deserializeValue(retVal->tableID);
    return retVal;
}

void DropCatalogEntryRecord::serialize(Serializer& serializer) const {
    WALRecord::serialize(serializer);
    serializer.write(entryID);
}

std::unique_ptr<DropCatalogEntryRecord> DropCatalogEntryRecord::deserialize(
    Deserializer& deserializer) {
    auto retVal = std::make_unique<DropCatalogEntryRecord>();
    deserializer.deserializeValue(retVal->entryID);
    return retVal;
}

static void serializeAlterExtraInfo(Serializer& serializer, const BoundAlterInfo* alterInfo) {
    const auto* extraInfo = alterInfo->extraInfo.get();
    serializer.write(alterInfo->alterType);
    serializer.write(alterInfo->tableName);
    switch (alterInfo->alterType) {
    case AlterType::ADD_PROPERTY: {
        auto addInfo = extraInfo->constPtrCast<BoundExtraAddPropertyInfo>();
        addInfo->propertyDefinition.serialize(serializer);
    } break;
    case AlterType::DROP_PROPERTY: {
        auto dropInfo = extraInfo->constPtrCast<BoundExtraDropPropertyInfo>();
        serializer.write(dropInfo->propertyName);
    } break;
    case AlterType::RENAME_PROPERTY: {
        auto renamePropertyInfo = extraInfo->constPtrCast<BoundExtraRenamePropertyInfo>();
        serializer.write(renamePropertyInfo->newName);
        serializer.write(renamePropertyInfo->oldName);
    } break;
    case AlterType::COMMENT: {
        auto commentInfo = extraInfo->constPtrCast<BoundExtraCommentInfo>();
        serializer.write(commentInfo->comment);
    } break;
    case AlterType::RENAME: {
        auto renameTableInfo = extraInfo->constPtrCast<BoundExtraRenameTableInfo>();
        serializer.write(renameTableInfo->newName);
    } break;
    case AlterType::ADD_FROM_TO_CONNECTION: {
        auto fromToConnectionInfo = extraInfo->constPtrCast<BoundExtraAddFromToConnection>();
        serializer.write(fromToConnectionInfo->srcTableID);
        serializer.write(fromToConnectionInfo->dstTableID);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

static decltype(auto) deserializeAlterRecord(Deserializer& deserializer) {
    auto alterType = AlterType::INVALID;
    std::string tableName;
    deserializer.deserializeValue(alterType);
    deserializer.deserializeValue(tableName);
    std::unique_ptr<BoundExtraAlterInfo> extraInfo;
    switch (alterType) {
    case AlterType::ADD_PROPERTY: {
        auto definition = PropertyDefinition::deserialize(deserializer);
        extraInfo = std::make_unique<BoundExtraAddPropertyInfo>(std::move(definition), nullptr);
    } break;
    case AlterType::DROP_PROPERTY: {
        std::string propertyName;
        deserializer.deserializeValue(propertyName);
        extraInfo = std::make_unique<BoundExtraDropPropertyInfo>(std::move(propertyName));
    } break;
    case AlterType::RENAME_PROPERTY: {
        std::string newName;
        std::string oldName;
        deserializer.deserializeValue(newName);
        deserializer.deserializeValue(oldName);
        extraInfo =
            std::make_unique<BoundExtraRenamePropertyInfo>(std::move(newName), std::move(oldName));
    } break;
    case AlterType::COMMENT: {
        std::string comment;
        deserializer.deserializeValue(comment);
        extraInfo = std::make_unique<BoundExtraCommentInfo>(std::move(comment));
    } break;
    case AlterType::RENAME: {
        std::string newName;
        deserializer.deserializeValue(newName);
        extraInfo = std::make_unique<BoundExtraRenameTableInfo>(std::move(newName));
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return std::make_tuple(alterType, tableName, std::move(extraInfo));
}

void AlterTableEntryRecord::serialize(Serializer& serializer) const {
    WALRecord::serialize(serializer);
    serializeAlterExtraInfo(serializer, alterInfo);
}

std::unique_ptr<AlterTableEntryRecord> AlterTableEntryRecord::deserialize(
    Deserializer& deserializer) {
    auto [alterType, tableName, extraInfo] = deserializeAlterRecord(deserializer);
    auto retval = std::make_unique<AlterTableEntryRecord>();
    retval->ownedAlterInfo =
        std::make_unique<BoundAlterInfo>(alterType, tableName, std::move(extraInfo));
    return retval;
}

void UpdateSequenceRecord::serialize(Serializer& serializer) const {
    WALRecord::serialize(serializer);
    serializer.write(sequenceID);
    serializer.write(kCount);
}

std::unique_ptr<UpdateSequenceRecord> UpdateSequenceRecord::deserialize(
    Deserializer& deserializer) {
    auto retVal = std::make_unique<UpdateSequenceRecord>();
    deserializer.deserializeValue(retVal->sequenceID);
    deserializer.deserializeValue(retVal->kCount);
    return retVal;
}

void TableInsertionRecord::serialize(Serializer& serializer) const {
    WALRecord::serialize(serializer);
    serializer.writeDebuggingInfo("table_id");
    serializer.write<table_id_t>(tableID);
    serializer.writeDebuggingInfo("table_type");
    serializer.write<TableType>(tableType);
    serializer.writeDebuggingInfo("num_rows");
    serializer.write<row_idx_t>(numRows);
    serializer.writeDebuggingInfo("num_vectors");
    serializer.write<idx_t>(vectors.size());
    for (auto& vector : vectors) {
        vector->serialize(serializer);
    }
}

std::unique_ptr<TableInsertionRecord> TableInsertionRecord::deserialize(Deserializer& deserializer,
    const main::ClientContext& clientContext) {
    std::string key;
    table_id_t tableID = INVALID_TABLE_ID;
    auto tableType = TableType::UNKNOWN;
    row_idx_t numRows = INVALID_ROW_IDX;
    idx_t numVectors = 0;
    std::vector<std::unique_ptr<ValueVector>> valueVectors;
    deserializer.validateDebuggingInfo(key, "table_id");
    deserializer.deserializeValue<table_id_t>(tableID);
    deserializer.validateDebuggingInfo(key, "table_type");
    deserializer.deserializeValue<TableType>(tableType);
    deserializer.validateDebuggingInfo(key, "num_rows");
    deserializer.deserializeValue<row_idx_t>(numRows);
    deserializer.validateDebuggingInfo(key, "num_vectors");
    deserializer.deserializeValue(numVectors);
    auto resultChunkState = std::make_shared<DataChunkState>();
    valueVectors.reserve(numVectors);
    for (auto i = 0u; i < numVectors; i++) {
        valueVectors.push_back(ValueVector::deSerialize(deserializer,
            clientContext.getMemoryManager(), resultChunkState));
    }
    return std::make_unique<TableInsertionRecord>(tableID, tableType, numRows,
        std::move(valueVectors));
}

void NodeDeletionRecord::serialize(Serializer& serializer) const {
    WALRecord::serialize(serializer);
    serializer.writeDebuggingInfo("table_id");
    serializer.write<table_id_t>(tableID);
    serializer.writeDebuggingInfo("node_offset");
    serializer.write<offset_t>(nodeOffset);
    serializer.writeDebuggingInfo("pk_vector");
    pkVector->serialize(serializer);
}

std::unique_ptr<NodeDeletionRecord> NodeDeletionRecord::deserialize(Deserializer& deserializer,
    const main::ClientContext& clientContext) {
    std::string key;
    table_id_t tableID = INVALID_TABLE_ID;
    offset_t nodeOffset = INVALID_OFFSET;

    deserializer.validateDebuggingInfo(key, "table_id");
    deserializer.deserializeValue<table_id_t>(tableID);
    deserializer.validateDebuggingInfo(key, "node_offset");
    deserializer.deserializeValue<offset_t>(nodeOffset);
    deserializer.validateDebuggingInfo(key, "pk_vector");
    auto resultChunkState = std::make_shared<DataChunkState>();
    auto ownedVector =
        ValueVector::deSerialize(deserializer, clientContext.getMemoryManager(), resultChunkState);
    return std::make_unique<NodeDeletionRecord>(tableID, nodeOffset, std::move(ownedVector));
}

void NodeUpdateRecord::serialize(Serializer& serializer) const {
    WALRecord::serialize(serializer);
    serializer.writeDebuggingInfo("table_id");
    serializer.write<table_id_t>(tableID);
    serializer.writeDebuggingInfo("column_id");
    serializer.write<column_id_t>(columnID);
    serializer.writeDebuggingInfo("node_offset");
    serializer.write<offset_t>(nodeOffset);
    serializer.writeDebuggingInfo("property_vector");
    propertyVector->serialize(serializer);
}

std::unique_ptr<NodeUpdateRecord> NodeUpdateRecord::deserialize(Deserializer& deserializer,
    const main::ClientContext& clientContext) {
    std::string key;
    table_id_t tableID = INVALID_TABLE_ID;
    column_id_t columnID = INVALID_COLUMN_ID;
    offset_t nodeOffset = INVALID_OFFSET;

    deserializer.validateDebuggingInfo(key, "table_id");
    deserializer.deserializeValue<table_id_t>(tableID);
    deserializer.validateDebuggingInfo(key, "column_id");
    deserializer.deserializeValue<column_id_t>(columnID);
    deserializer.validateDebuggingInfo(key, "node_offset");
    deserializer.deserializeValue<offset_t>(nodeOffset);
    deserializer.validateDebuggingInfo(key, "property_vector");
    auto resultChunkState = std::make_shared<DataChunkState>();
    auto ownedVector =
        ValueVector::deSerialize(deserializer, clientContext.getMemoryManager(), resultChunkState);
    return std::make_unique<NodeUpdateRecord>(tableID, columnID, nodeOffset,
        std::move(ownedVector));
}

void RelDeletionRecord::serialize(Serializer& serializer) const {
    WALRecord::serialize(serializer);
    serializer.writeDebuggingInfo("table_id");
    serializer.write<table_id_t>(tableID);
    serializer.writeDebuggingInfo("src_node_vector");
    srcNodeIDVector->serialize(serializer);
    serializer.writeDebuggingInfo("dst_node_vector");
    dstNodeIDVector->serialize(serializer);
    serializer.writeDebuggingInfo("rel_id_vector");
    relIDVector->serialize(serializer);
}

std::unique_ptr<RelDeletionRecord> RelDeletionRecord::deserialize(Deserializer& deserializer,
    const main::ClientContext& clientContext) {
    std::string key;
    table_id_t tableID = INVALID_TABLE_ID;

    deserializer.validateDebuggingInfo(key, "table_id");
    deserializer.deserializeValue<table_id_t>(tableID);
    deserializer.validateDebuggingInfo(key, "src_node_vector");
    auto resultChunkState = std::make_shared<DataChunkState>();
    auto srcNodeIDVector =
        ValueVector::deSerialize(deserializer, clientContext.getMemoryManager(), resultChunkState);
    deserializer.validateDebuggingInfo(key, "dst_node_vector");
    auto dstNodeIDVector =
        ValueVector::deSerialize(deserializer, clientContext.getMemoryManager(), resultChunkState);
    deserializer.validateDebuggingInfo(key, "rel_id_vector");
    auto relIDVector =
        ValueVector::deSerialize(deserializer, clientContext.getMemoryManager(), resultChunkState);
    return std::make_unique<RelDeletionRecord>(tableID, std::move(srcNodeIDVector),
        std::move(dstNodeIDVector), std::move(relIDVector));
}

void RelDetachDeleteRecord::serialize(Serializer& serializer) const {
    WALRecord::serialize(serializer);
    serializer.writeDebuggingInfo("table_id");
    serializer.write<table_id_t>(tableID);
    serializer.writeDebuggingInfo("direction");
    serializer.write<RelDataDirection>(direction);
    serializer.writeDebuggingInfo("src_node_vector");
    srcNodeIDVector->serialize(serializer);
}

std::unique_ptr<RelDetachDeleteRecord> RelDetachDeleteRecord::deserialize(
    Deserializer& deserializer, const main::ClientContext& clientContext) {
    std::string key;
    table_id_t tableID = INVALID_TABLE_ID;
    auto direction = RelDataDirection::INVALID;

    deserializer.validateDebuggingInfo(key, "table_id");
    deserializer.deserializeValue<table_id_t>(tableID);
    deserializer.validateDebuggingInfo(key, "direction");
    deserializer.deserializeValue<RelDataDirection>(direction);
    deserializer.validateDebuggingInfo(key, "src_node_vector");
    auto resultChunkState = std::make_shared<DataChunkState>();
    auto srcNodeIDVector =
        ValueVector::deSerialize(deserializer, clientContext.getMemoryManager(), resultChunkState);
    return std::make_unique<RelDetachDeleteRecord>(tableID, direction, std::move(srcNodeIDVector));
}

void RelUpdateRecord::serialize(Serializer& serializer) const {
    WALRecord::serialize(serializer);
    serializer.writeDebuggingInfo("table_id");
    serializer.write<table_id_t>(tableID);
    serializer.writeDebuggingInfo("column_id");
    serializer.write<column_id_t>(columnID);
    serializer.writeDebuggingInfo("src_node_vector");
    srcNodeIDVector->serialize(serializer);
    serializer.writeDebuggingInfo("dst_node_vector");
    dstNodeIDVector->serialize(serializer);
    serializer.writeDebuggingInfo("rel_id_vector");
    relIDVector->serialize(serializer);
    serializer.writeDebuggingInfo("property_vector");
    propertyVector->serialize(serializer);
}

std::unique_ptr<RelUpdateRecord> RelUpdateRecord::deserialize(Deserializer& deserializer,
    const main::ClientContext& clientContext) {
    std::string key;
    table_id_t tableID = INVALID_TABLE_ID;
    column_id_t columnID = INVALID_COLUMN_ID;

    deserializer.validateDebuggingInfo(key, "table_id");
    deserializer.deserializeValue<table_id_t>(tableID);
    deserializer.validateDebuggingInfo(key, "column_id");
    deserializer.deserializeValue<column_id_t>(columnID);
    deserializer.validateDebuggingInfo(key, "src_node_vector");
    auto resultChunkState = std::make_shared<DataChunkState>();
    auto srcNodeIDVector =
        ValueVector::deSerialize(deserializer, clientContext.getMemoryManager(), resultChunkState);
    deserializer.validateDebuggingInfo(key, "dst_node_vector");
    auto dstNodeIDVector =
        ValueVector::deSerialize(deserializer, clientContext.getMemoryManager(), resultChunkState);
    deserializer.validateDebuggingInfo(key, "rel_id_vector");
    auto relIDVector =
        ValueVector::deSerialize(deserializer, clientContext.getMemoryManager(), resultChunkState);
    deserializer.validateDebuggingInfo(key, "property_vector");
    auto propertyVector =
        ValueVector::deSerialize(deserializer, clientContext.getMemoryManager(), resultChunkState);
    return std::make_unique<RelUpdateRecord>(tableID, columnID, std::move(srcNodeIDVector),
        std::move(dstNodeIDVector), std::move(relIDVector), std::move(propertyVector));
}

} // namespace storage
} // namespace kuzu
