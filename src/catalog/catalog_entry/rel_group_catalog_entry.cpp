#include "catalog/catalog_entry/rel_group_catalog_entry.h"

#include <sstream>

#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/serializer/deserializer.h"
#include "main/client_context.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace catalog {

std::unique_ptr<RelGroupCatalogEntry> RelGroupCatalogEntry::alter(transaction_t timestamp,
    const binder::BoundAlterInfo& alterInfo) const {
    std::unique_ptr<RelGroupCatalogEntry> newEntry;
    switch (alterInfo.alterType) {
    case AlterType::RENAME: {
        newEntry = copy();
        auto& renameTableInfo =
            *alterInfo.extraInfo->constPtrCast<binder::BoundExtraRenameTableInfo>();
        newEntry->rename(renameTableInfo.newName);
        newEntry->setTimestamp(timestamp);
        newEntry->setOID(oid);
    } break;
    case AlterType::COMMENT: {
        newEntry = copy();
        auto& commentInfo = *alterInfo.extraInfo->constPtrCast<binder::BoundExtraCommentInfo>();
        newEntry->setComment(commentInfo.comment);
        newEntry->setTimestamp(timestamp);
        newEntry->setOID(oid);
    } break;
    default: {
        // the only alter types needed to be handled in the rel group are rename and comment.
        // the rest is handled in the child member tables.
    }
    }
    return newEntry;
}

bool RelGroupCatalogEntry::isParent(table_id_t tableID) const {
    const auto it = find_if(relTableIDs.begin(), relTableIDs.end(),
        [&](table_id_t relTableID) { return relTableID == tableID; });
    return it != relTableIDs.end();
}

void RelGroupCatalogEntry::serialize(Serializer& serializer) const {
    CatalogEntry::serialize(serializer);
    serializer.writeDebuggingInfo("relTableIDs");
    serializer.serializeVector(relTableIDs);
    serializer.writeDebuggingInfo("comment");
    serializer.serializeValue(comment);
}

std::unique_ptr<RelGroupCatalogEntry> RelGroupCatalogEntry::deserialize(
    Deserializer& deserializer) {
    std::string debuggingInfo;
    std::vector<table_id_t> relTableIDs;
    std::string comment;
    deserializer.validateDebuggingInfo(debuggingInfo, "relTableIDs");
    deserializer.deserializeVector(relTableIDs);
    deserializer.validateDebuggingInfo(debuggingInfo, "comment");
    deserializer.deserializeValue(comment);
    auto relGroupEntry = std::make_unique<RelGroupCatalogEntry>();
    relGroupEntry->relTableIDs = std::move(relTableIDs);
    relGroupEntry->comment = comment;
    return relGroupEntry;
}

binder::BoundCreateTableInfo RelGroupCatalogEntry::getBoundCreateTableInfo(
    transaction::Transaction* transaction, const Catalog* catalog, bool isInternal) const {
    std::vector<binder::BoundCreateTableInfo> infos;
    for (auto relTableID : relTableIDs) {
        auto relEntry = catalog->getTableCatalogEntry(transaction, relTableID);
        KU_ASSERT(relEntry != nullptr);
        auto boundInfo = relEntry->getBoundCreateTableInfo(transaction, false);
        boundInfo.hasParent = true;
        infos.push_back(std::move(boundInfo));
    }
    auto extraInfo = std::make_unique<binder::BoundExtraCreateRelTableGroupInfo>(std::move(infos));
    return binder::BoundCreateTableInfo(type, name, ConflictAction::ON_CONFLICT_THROW,
        std::move(extraInfo), isInternal);
}

static std::string getFromToStr(table_id_t tableID, Catalog* catalog,
    const transaction::Transaction* transaction) {
    auto& entry =
        catalog->getTableCatalogEntry(transaction, tableID)->constCast<RelTableCatalogEntry>();
    auto srcTableName =
        catalog->getTableCatalogEntry(transaction, entry.getSrcTableID())->getName();
    auto dstTableName =
        catalog->getTableCatalogEntry(transaction, entry.getDstTableID())->getName();
    return stringFormat("FROM {} TO {}", srcTableName, dstTableName);
}

std::string RelGroupCatalogEntry::toCypher(const ToCypherInfo& info) const {
    auto relGroupInfo = info.constCast<RelGroupToCypherInfo>();
    auto catalog = relGroupInfo.context->getCatalog();
    auto transaction = relGroupInfo.context->getTransaction();
    std::stringstream ss;
    ss << stringFormat("CREATE REL TABLE `{}` (", getName());
    KU_ASSERT(!relTableIDs.empty());
    ss << getFromToStr(relTableIDs[0], catalog, transaction);
    for (auto i = 1u; i < relTableIDs.size(); ++i) {
        ss << stringFormat(", {}", getFromToStr(relTableIDs[i], catalog, transaction));
    }
    auto childEntry =
        catalog->getTableCatalogEntry(transaction, relTableIDs[0])->ptrCast<RelTableCatalogEntry>();
    ss << ", " << childEntry->propertiesToCypher() << childEntry->getMultiplicityStr() << ");";
    return ss.str();
}

} // namespace catalog
} // namespace kuzu
