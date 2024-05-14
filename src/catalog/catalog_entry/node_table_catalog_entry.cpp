#include "catalog/catalog_entry/node_table_catalog_entry.h"

#include "binder/ddl/bound_create_table_info.h"

using namespace kuzu::binder;

namespace kuzu {
namespace catalog {

NodeTableCatalogEntry::NodeTableCatalogEntry(CatalogSet* set, std::string name,
    common::table_id_t tableID, common::property_id_t primaryKeyPID)
    : TableCatalogEntry{set, CatalogEntryType::NODE_TABLE_ENTRY, std::move(name), tableID},
      primaryKeyPID{primaryKeyPID} {}

void NodeTableCatalogEntry::serialize(common::Serializer& serializer) const {
    TableCatalogEntry::serialize(serializer);
    serializer.write(primaryKeyPID);
    serializer.serializeUnorderedSet(fwdRelTableIDSet);
    serializer.serializeUnorderedSet(bwdRelTableIDSet);
}

std::unique_ptr<NodeTableCatalogEntry> NodeTableCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
    common::property_id_t primaryKeyPID;
    common::table_id_set_t fwdRelTableIDSet;
    common::table_id_set_t bwdRelTableIDSet;
    deserializer.deserializeValue(primaryKeyPID);
    deserializer.deserializeUnorderedSet(fwdRelTableIDSet);
    deserializer.deserializeUnorderedSet(bwdRelTableIDSet);
    auto nodeTableEntry = std::make_unique<NodeTableCatalogEntry>();
    nodeTableEntry->primaryKeyPID = primaryKeyPID;
    nodeTableEntry->fwdRelTableIDSet = std::move(fwdRelTableIDSet);
    nodeTableEntry->bwdRelTableIDSet = std::move(bwdRelTableIDSet);
    return nodeTableEntry;
}

std::string NodeTableCatalogEntry::toCypher(main::ClientContext* /*clientContext*/) const {
    return common::stringFormat("CREATE NODE TABLE {} ({} PRIMARY KEY({}));", getName(),
        Property::toCypher(getPropertiesRef()), getPrimaryKey()->getName());
}

std::unique_ptr<TableCatalogEntry> NodeTableCatalogEntry::copy() const {
    auto other = std::make_unique<NodeTableCatalogEntry>();
    other->primaryKeyPID = primaryKeyPID;
    other->fwdRelTableIDSet = fwdRelTableIDSet;
    other->bwdRelTableIDSet = bwdRelTableIDSet;
    other->copyFrom(*this);
    return other;
}

std::unique_ptr<BoundExtraCreateCatalogEntryInfo> NodeTableCatalogEntry::getBoundExtraCreateInfo(
    transaction::Transaction*) const {
    std::vector<PropertyInfo> propertyInfos;
    for (const auto& property : properties) {
        propertyInfos.emplace_back(property.getName(), *property.getDataType());
    }
    auto result =
        std::make_unique<BoundExtraCreateNodeTableInfo>(primaryKeyPID, std::move(propertyInfos));
    return result;
}

} // namespace catalog
} // namespace kuzu
