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

bool RelGroupCatalogEntry::isParent(common::table_id_t tableID) const {
    const auto it = find_if(relTableIDs.begin(), relTableIDs.end(),
        [&](table_id_t relTableID) { return relTableID == tableID; });
    return it != relTableIDs.end();
}

void RelGroupCatalogEntry::serialize(Serializer& serializer) const {
    CatalogEntry::serialize(serializer);
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

binder::BoundCreateTableInfo RelGroupCatalogEntry::getBoundCreateTableInfo(
    transaction::Transaction* transaction, Catalog* catalog, bool isInternal) const {
    std::vector<binder::BoundCreateTableInfo> infos;
    for (auto relTableID : relTableIDs) {
        auto relEntry = catalog->getTableCatalogEntry(transaction, relTableID);
        KU_ASSERT(relEntry != nullptr);
        auto boundInfo = relEntry->getBoundCreateTableInfo(transaction, false);
        infos.push_back(std::move(boundInfo));
    }
    auto extraInfo = std::make_unique<binder::BoundExtraCreateRelTableGroupInfo>(std::move(infos));
    return binder::BoundCreateTableInfo(type, name, ConflictAction::ON_CONFLICT_THROW,
        std::move(extraInfo), isInternal);
}

static std::string getFromToStr(common::table_id_t tableID, ClientContext* context) {
    auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
    auto& entry =
        catalog->getTableCatalogEntry(transaction, tableID)->constCast<RelTableCatalogEntry>();
    auto srcTableName =
        catalog->getTableCatalogEntry(transaction, entry.getSrcTableID())->getName();
    auto dstTableName =
        catalog->getTableCatalogEntry(transaction, entry.getDstTableID())->getName();
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
    auto childRelEntry = clientContext->getCatalog()->getTableCatalogEntry(
        clientContext->getTransaction(), relTableIDs[0]);
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
