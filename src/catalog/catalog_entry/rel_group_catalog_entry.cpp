#include "catalog/catalog_entry/rel_group_catalog_entry.h"

#include <sstream>

#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "catalog/catalog_set.h"
#include "common/serializer/deserializer.h"
#include "main/client_context.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace catalog {

RelGroupCatalogEntry::RelGroupCatalogEntry(CatalogSet* set, std::string tableName,
    std::vector<table_id_t> relTableIDs)
    : TableCatalogEntry{set, CatalogEntryType::REL_GROUP_ENTRY, std::move(tableName)},
      relTableIDs{std::move(relTableIDs)} {}

bool RelGroupCatalogEntry::isParent(table_id_t childID) {
    const auto it = find_if(relTableIDs.begin(), relTableIDs.end(),
        [&](table_id_t relTableID) { return relTableID == childID; });
    return it != relTableIDs.end();
}

void RelGroupCatalogEntry::serialize(Serializer& serializer) const {
    TableCatalogEntry::serialize(serializer);
    serializer.writeDebuggingInfo("relTableIDs");
    serializer.serializeVector(relTableIDs);
}

std::unique_ptr<RelGroupCatalogEntry> RelGroupCatalogEntry::deserialize(
    Deserializer& deserializer) {
    std::string debuggingInfo;
    std::vector<table_id_t> relTableIDs;
    deserializer.validateDebuggingInfo(debuggingInfo, "relTableIDs");
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
    transaction::Transaction* transaction, const CatalogEntrySet& entries, table_id_t tableID) {
    for (auto& [name, entry] : entries) {
        auto current = ku_dynamic_cast<TableCatalogEntry*>(entry);
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

static std::string getFromToStr(common::table_id_t tableID, ClientContext* context) {
    auto catalog = context->getCatalog();
    auto transaction = context->getTx();
    auto& entry =
        catalog->getTableCatalogEntry(transaction, tableID)->constCast<RelTableCatalogEntry>();
    auto srcTableName = catalog->getTableName(transaction, entry.getSrcTableID());
    auto dstTableName = catalog->getTableName(transaction, entry.getDstTableID());
    return stringFormat("FROM {} TO {}", srcTableName, dstTableName);
}

std::string RelGroupCatalogEntry::toCypher(ClientContext* clientContext) const {
    std::stringstream ss;
    ss << stringFormat("CREATE REL TABLE GROUP {} ( ", getName());
    KU_ASSERT(!relTableIDs.empty());
    ss << getFromToStr(relTableIDs[0], clientContext);
    for (auto i = 1u; i < relTableIDs.size(); ++i) {
        ss << stringFormat(", {}", getFromToStr(relTableIDs[i], clientContext));
    }
    auto childRelEntry =
        clientContext->getCatalog()->getTableCatalogEntry(clientContext->getTx(), relTableIDs[0]);
    if (childRelEntry->getNumProperties() > 1) { // skip internal id property.
        auto propertyStr = stringFormat(", {}", childRelEntry->propertiesToCypher());
        propertyStr.resize(propertyStr.size() - 1);
        ss << propertyStr;
    }
    ss << ");";
    return ss.str();
}

} // namespace catalog
} // namespace kuzu
