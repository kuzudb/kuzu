#include "catalog/catalog_entry/node_table_catalog_entry.h"

#include <sstream>

namespace kuzu {
namespace catalog {

NodeTableCatalogEntry::NodeTableCatalogEntry(
    std::string name, common::table_id_t tableID, common::property_id_t primaryKeyPID)
    : TableCatalogEntry{CatalogEntryType::NODE_TABLE_ENTRY, std::move(name), tableID},
      primaryKeyPID{primaryKeyPID} {}

NodeTableCatalogEntry::NodeTableCatalogEntry(const NodeTableCatalogEntry& other)
    : TableCatalogEntry{other} {
    primaryKeyPID = other.primaryKeyPID;
    fwdRelTableIDSet = other.fwdRelTableIDSet;
    bwdRelTableIDSet = other.bwdRelTableIDSet;
}

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

std::unique_ptr<CatalogEntry> NodeTableCatalogEntry::copy() const {
    return std::make_unique<NodeTableCatalogEntry>(*this);
}

std::string NodeTableCatalogEntry::toCypher(main::ClientContext* /*clientContext*/) const {
    std::stringstream ss;
    ss << "CREATE NODE TABLE " << getName() << "(";
    Property::toCypher(getPropertiesRef(), ss);
    ss << " PRIMARY KEY(" << getPrimaryKey()->getName() << "));";
    return ss.str();
}

} // namespace catalog
} // namespace kuzu
