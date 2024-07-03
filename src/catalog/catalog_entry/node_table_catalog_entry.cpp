#include "catalog/catalog_entry/node_table_catalog_entry.h"

#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog_set.h"

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
}

std::unique_ptr<NodeTableCatalogEntry> NodeTableCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
    common::property_id_t primaryKeyPID;
    deserializer.deserializeValue(primaryKeyPID);
    auto nodeTableEntry = std::make_unique<NodeTableCatalogEntry>();
    nodeTableEntry->primaryKeyPID = primaryKeyPID;
    return nodeTableEntry;
}

std::string NodeTableCatalogEntry::toCypher(main::ClientContext* /*clientContext*/) const {
    return common::stringFormat("CREATE NODE TABLE {} ({} PRIMARY KEY({}));", getName(),
        Property::toCypher(getPropertiesRef()), getPrimaryKey()->getName());
}

std::unique_ptr<TableCatalogEntry> NodeTableCatalogEntry::copy() const {
    auto other = std::make_unique<NodeTableCatalogEntry>();
    other->primaryKeyPID = primaryKeyPID;
    other->copyFrom(*this);
    return other;
}

std::unique_ptr<BoundExtraCreateCatalogEntryInfo> NodeTableCatalogEntry::getBoundExtraCreateInfo(
    transaction::Transaction*) const {
    std::vector<PropertyInfo> propertyInfos;
    for (const auto& property : properties) {
        propertyInfos.emplace_back(property.getName(), property.getDataType().copy(),
            property.getDefaultExpr()->copy());
    }
    auto result =
        std::make_unique<BoundExtraCreateNodeTableInfo>(primaryKeyPID, std::move(propertyInfos));
    return result;
}

} // namespace catalog
} // namespace kuzu
