#include "catalog/catalog_entry/node_table_catalog_entry.h"

#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "catalog/catalog_set.h"
#include "common/serializer/deserializer.h"

using namespace kuzu::binder;

namespace kuzu {
namespace catalog {

common::table_id_set_t NodeTableCatalogEntry::getFwdRelTableIDs(Catalog* catalog,
    transaction::Transaction* transaction) const {
    common::table_id_set_t result;
    for (const auto& relEntry : catalog->getRelTableEntries(transaction)) {
        if (relEntry->getSrcTableID() == getTableID()) {
            result.insert(relEntry->getTableID());
        }
    }
    return result;
}

common::table_id_set_t NodeTableCatalogEntry::getBwdRelTableIDs(Catalog* catalog,
    transaction::Transaction* transaction) const {
    common::table_id_set_t result;
    for (const auto& relEntry : catalog->getRelTableEntries(transaction)) {
        if (relEntry->getDstTableID() == getTableID()) {
            result.insert(relEntry->getTableID());
        }
    }
    return result;
}

void NodeTableCatalogEntry::serialize(common::Serializer& serializer) const {
    TableCatalogEntry::serialize(serializer);
    serializer.writeDebuggingInfo("primaryKeyName");
    serializer.write(primaryKeyName);
}

std::unique_ptr<NodeTableCatalogEntry> NodeTableCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
    std::string debuggingInfo;
    std::string primaryKeyName;
    deserializer.validateDebuggingInfo(debuggingInfo, "primaryKeyName");
    deserializer.deserializeValue(primaryKeyName);
    auto nodeTableEntry = std::make_unique<NodeTableCatalogEntry>();
    nodeTableEntry->primaryKeyName = primaryKeyName;
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
