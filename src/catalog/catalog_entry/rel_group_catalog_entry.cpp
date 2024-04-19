#include "catalog/catalog_entry/rel_group_catalog_entry.h"

#include <sstream>

#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace catalog {

RelGroupCatalogEntry::RelGroupCatalogEntry(std::string tableName, common::table_id_t tableID,
    std::vector<common::table_id_t> relTableIDs)
    : TableCatalogEntry{CatalogEntryType::REL_GROUP_ENTRY, std::move(tableName), tableID},
      relTableIDs{std::move(relTableIDs)} {}

RelGroupCatalogEntry::RelGroupCatalogEntry(const RelGroupCatalogEntry& other)
    : TableCatalogEntry{other} {
    relTableIDs = other.relTableIDs;
}

bool RelGroupCatalogEntry::isParent(common::table_id_t childID) {
    auto it = find_if(relTableIDs.begin(), relTableIDs.end(),
        [&](common::table_id_t relTableID) { return relTableID == childID; });
    return it != relTableIDs.end();
}

void RelGroupCatalogEntry::serialize(common::Serializer& serializer) const {
    TableCatalogEntry::serialize(serializer);
    serializer.serializeVector(relTableIDs);
}

std::unique_ptr<RelGroupCatalogEntry> RelGroupCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
    std::vector<common::table_id_t> relTableIDs;
    deserializer.deserializeVector(relTableIDs);
    auto relGroupEntry = std::make_unique<RelGroupCatalogEntry>();
    relGroupEntry->relTableIDs = std::move(relTableIDs);
    return relGroupEntry;
}

std::unique_ptr<CatalogEntry> RelGroupCatalogEntry::copy() const {
    return std::make_unique<RelGroupCatalogEntry>(*this);
}

std::string RelGroupCatalogEntry::toCypher(ClientContext* clientContext) const {
    std::stringstream ss;
    auto catalog = clientContext->getCatalog();
    ss << stringFormat("CREATE REL TABLE GROUP {} ( ", getName());
    RelTableCatalogEntry* relTableEntry;
    for (auto relTableID : relTableIDs) {
        relTableEntry = ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(
            catalog->getTableCatalogEntry(clientContext->getTx(), relTableID));
        auto srcTableName =
            catalog->getTableName(clientContext->getTx(), relTableEntry->getSrcTableID());
        auto dstTableName =
            catalog->getTableName(clientContext->getTx(), relTableEntry->getDstTableID());
        ss << stringFormat("FROM {} TO {}, ", srcTableName, dstTableName);
    }
    auto prop = Property::toCypher(relTableEntry->getPropertiesRef());
    if (prop.size() > 1)
        prop.resize(prop.size() - 1);
    ss << prop << ");";
    return ss.str();
}

} // namespace catalog
} // namespace kuzu
