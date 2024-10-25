#include "catalog/catalog_entry/node_table_catalog_entry.h"

#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog_set.h"
#include "common/serializer/deserializer.h"

using namespace kuzu::binder;

namespace kuzu {
namespace catalog {

std::unique_ptr<TableCatalogEntry> NodeTableCatalogEntry::alter(
    const binder::BoundAlterInfo& alterInfo) const {
    KU_ASSERT(!deleted);
    switch (alterInfo.alterType) {
    case common::AlterType::ADD_INDEX: {
        auto& indexInfo = *alterInfo.extraInfo->constPtrCast<BoundExtraIndexInfo>();
        auto newEntry = copy();
        newEntry->ptrCast<NodeTableCatalogEntry>()->addIndex(indexInfo.indexName);
        return newEntry;
    }
    case common::AlterType::DROP_INDEX: {
        auto& indexInfo = *alterInfo.extraInfo->constPtrCast<BoundExtraIndexInfo>();
        auto newEntry = copy();
        newEntry->ptrCast<NodeTableCatalogEntry>()->dropIndex(indexInfo.indexName);
        return newEntry;
    }
    default: {
        return TableCatalogEntry::alter(alterInfo);
    }
    }
}

void NodeTableCatalogEntry::serialize(common::Serializer& serializer) const {
    TableCatalogEntry::serialize(serializer);
    serializer.writeDebuggingInfo("primaryKeyName");
    serializer.write(primaryKeyName);
    serializer.serializeValue<uint64_t>(indexes.size());
    for (auto& [name, index] : indexes) {
        index->serialize(serializer);
    }
}

std::unique_ptr<NodeTableCatalogEntry> NodeTableCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
    std::string debuggingInfo;
    std::string primaryKeyName;
    deserializer.validateDebuggingInfo(debuggingInfo, "primaryKeyName");
    deserializer.deserializeValue(primaryKeyName);
    auto nodeTableEntry = std::make_unique<NodeTableCatalogEntry>();
    nodeTableEntry->primaryKeyName = primaryKeyName;
    uint64_t numIndexes = 0u;
    deserializer.deserializeValue(numIndexes);
    for (auto i = 0u; i < numIndexes; i++) {
        auto indexEntry = IndexCatalogEntry::deserialize(deserializer);
        nodeTableEntry->indexes.emplace(indexEntry->getName(), std::move(indexEntry));
    }
    return nodeTableEntry;
}

std::string NodeTableCatalogEntry::toCypher(main::ClientContext* /*clientContext*/) const {
    return common::stringFormat("CREATE NODE TABLE {} ({} PRIMARY KEY({}));", getName(),
        propertyCollection.toCypher(), primaryKeyName);
}

std::unique_ptr<TableCatalogEntry> NodeTableCatalogEntry::copy() const {
    auto other = std::make_unique<NodeTableCatalogEntry>();
    other->primaryKeyName = primaryKeyName;
    other->copyFrom(*this);
    return other;
}

std::unique_ptr<BoundExtraCreateCatalogEntryInfo> NodeTableCatalogEntry::getBoundExtraCreateInfo(
    transaction::Transaction*) const {
    return std::make_unique<BoundExtraCreateNodeTableInfo>(primaryKeyName,
        copyVector(getProperties()));
}

} // namespace catalog
} // namespace kuzu
