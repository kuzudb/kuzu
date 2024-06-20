#include "catalog/catalog_entry/rel_group_catalog_entry.h"

#include <sstream>

#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "catalog/catalog_set.h"
#include "main/client_context.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace catalog {

RelGroupCatalogEntry::RelGroupCatalogEntry(CatalogSet* set, std::string tableName,
    common::table_id_t tableID, std::vector<common::table_id_t> relTableIDs)
    : TableCatalogEntry{set, CatalogEntryType::REL_GROUP_ENTRY, std::move(tableName), tableID},
      relTableIDs{std::move(relTableIDs)} {}

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

std::unique_ptr<TableCatalogEntry> RelGroupCatalogEntry::copy() const {
    auto other = std::make_unique<RelGroupCatalogEntry>();
    other->relTableIDs = relTableIDs;
    other->copyFrom(*this);
    return other;
}

static std::optional<binder::BoundCreateTableInfo> getBoundCreateTableInfoForTable(
    transaction::Transaction* transaction, CatalogEntrySet entries, common::table_id_t tableID) {
    for (auto& [name, entry] : entries) {
        auto current = common::ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(entry);
        if (current->getTableID() == tableID) {
            auto boundInfo = current->getBoundCreateTableInfo(transaction);
            return boundInfo;
        }
    }
    return std::nullopt;
}

std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo>
RelGroupCatalogEntry::getBoundExtraCreateInfo(transaction::Transaction* transaction) const {
    std::vector<binder::BoundCreateTableInfo> infos;
    auto entries = set->getEntries(transaction);
    for (auto relTableID : relTableIDs) {
        auto boundInfo = getBoundCreateTableInfoForTable(transaction, entries, relTableID);
        KU_ASSERT(boundInfo.has_value());
        infos.push_back(std::move(boundInfo.value()));
    }
    return std::make_unique<binder::BoundExtraCreateRelTableGroupInfo>(std::move(infos));
}

std::string RelGroupCatalogEntry::toCypher(ClientContext* clientContext) const {
    std::stringstream ss;
    auto catalog = clientContext->getCatalog();
    ss << stringFormat("CREATE REL TABLE GROUP {} ( ", getName());
    std::string prop;
    for (auto relTableID : relTableIDs) {
        auto relTableEntry = catalog->getTableCatalogEntry(clientContext->getTx(), relTableID)
                                 ->constPtrCast<RelTableCatalogEntry>();
        if (prop.empty())
            prop = Property::toCypher(relTableEntry->getPropertiesRef());
        auto srcTableName =
            catalog->getTableName(clientContext->getTx(), relTableEntry->getSrcTableID());
        auto dstTableName =
            catalog->getTableName(clientContext->getTx(), relTableEntry->getDstTableID());
        ss << stringFormat("FROM {} TO {}, ", srcTableName, dstTableName);
    }
    if (!prop.empty())
        prop.resize(prop.size() - 1);
    ss << prop << ");";
    return ss.str();
}

} // namespace catalog
} // namespace kuzu
