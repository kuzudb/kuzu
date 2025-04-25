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

void RelTableInfo::serialize(common::Serializer& ser) const {
    ser.writeDebuggingInfo("nodePair");
    ser.serializeValue(nodePair);
    ser.writeDebuggingInfo("oid");
    ser.serializeValue(oid);
}

RelTableInfo RelTableInfo::deserialize(common::Deserializer& deser) {
    std::string debuggingInfo;
    common::oid_t oid;
    deser.validateDebuggingInfo(debuggingInfo, "nodePair");
    auto nodePair = NodePair::deserialize(deser);
    deser.validateDebuggingInfo(debuggingInfo, "oid");
    deser.deserializeValue(oid);
    return RelTableInfo{nodePair, oid};
}

// std::unique_ptr<RelGroupCatalogEntry> RelGroupCatalogEntry::alter(transaction_t timestamp,
//     const binder::BoundAlterInfo& alterInfo) const {
//     std::unique_ptr<RelGroupCatalogEntry> newEntry;
//     switch (alterInfo.alterType) {
//     case AlterType::RENAME: {
//         newEntry = copy();
//         auto& renameTableInfo =
//             *alterInfo.extraInfo->constPtrCast<binder::BoundExtraRenameTableInfo>();
//         newEntry->rename(renameTableInfo.newName);
//         newEntry->setTimestamp(timestamp);
//         newEntry->setOID(oid);
//     } break;
//     case AlterType::COMMENT: {
//         newEntry = copy();
//         auto& commentInfo = *alterInfo.extraInfo->constPtrCast<binder::BoundExtraCommentInfo>();
//         newEntry->setComment(commentInfo.comment);
//         newEntry->setTimestamp(timestamp);
//         newEntry->setOID(oid);
//     } break;
//     default: {
//         // the only alter types needed to be handled in the rel group are rename and comment.
//         // the rest is handled in the child member tables.
//     }
//     }
//     return newEntry;
// }


void RelGroupCatalogEntry::serialize(Serializer& serializer) const {
    TableCatalogEntry::serialize(serializer);
    serializer.writeDebuggingInfo("srcMultiplicity");
    serializer.serializeValue(srcMultiplicity);
    serializer.writeDebuggingInfo("dstMultiplicity");
    serializer.serializeValue(dstMultiplicity);
    serializer.writeDebuggingInfo("storageDirection");
    serializer.serializeValue(storageDirection);
    serializer.writeDebuggingInfo("relTableInfos");
    serializer.serializeVector(relTableInfos);
}

std::unique_ptr<RelGroupCatalogEntry> RelGroupCatalogEntry::deserialize(
    Deserializer& deserializer) {
    std::string debuggingInfo;
    common::RelMultiplicity srcMultiplicity;
    common::RelMultiplicity dstMultiplicity;
    common::ExtendDirection storageDirection;
    std::vector<RelTableInfo> relTableInfos;
    deserializer.validateDebuggingInfo(debuggingInfo, "srcMultiplicity");
    deserializer.deserializeValue(srcMultiplicity);
    deserializer.validateDebuggingInfo(debuggingInfo, "dstMultiplicity");
    deserializer.deserializeValue(dstMultiplicity);
    deserializer.validateDebuggingInfo(debuggingInfo, "storageDirection");
    deserializer.deserializeValue(storageDirection);
    deserializer.validateDebuggingInfo(debuggingInfo, "relTableInfos");
    deserializer.deserializeVector(relTableInfos);
    auto relGroupEntry = std::make_unique<RelGroupCatalogEntry>();
    relGroupEntry->srcMultiplicity = srcMultiplicity;
    relGroupEntry->dstMultiplicity = dstMultiplicity;
    relGroupEntry->storageDirection = storageDirection;
    relGroupEntry->relTableInfos = relTableInfos;
    return relGroupEntry;
}

static std::string getFromToStr(table_id_t tableID, Catalog* catalog,
    const transaction::Transaction* transaction) {
    auto& entry =
        catalog->getTableCatalogEntry(transaction, tableID)->constCast<RelTableCatalogEntry>();
    auto srcTableName =
        catalog->getTableCatalogEntry(transaction, entry.getSrcTableID())->getName();
    auto dstTableName =
        catalog->getTableCatalogEntry(transaction, entry.getDstTableID())->getName();
    return stringFormat("FROM `{}` TO `{}`", srcTableName, dstTableName);
}

std::string RelGroupCatalogEntry::toCypher(const ToCypherInfo& info) const {
    //    auto relGroupInfo = info.constCast<RelGroupToCypherInfo>();
    //    auto catalog = relGroupInfo.context->getCatalog();
    //    auto transaction = relGroupInfo.context->getTransaction();
    //    std::stringstream ss;
    //    ss << stringFormat("CREATE REL TABLE `{}` (", getName());
    //    KU_ASSERT(!relTableIDs.empty());
    //    ss << getFromToStr(relTableIDs[0], catalog, transaction);
    //    for (auto i = 1u; i < relTableIDs.size(); ++i) {
    //        ss << stringFormat(", {}", getFromToStr(relTableIDs[i], catalog, transaction));
    //    }
    //    auto childEntry =
    //        catalog->getTableCatalogEntry(transaction,
    //        relTableIDs[0])->ptrCast<RelTableCatalogEntry>();
    //    ss << ", " << childEntry->propertiesToCypher() << childEntry->getMultiplicityStr() <<
    //    ");"; return ss.str();
}

std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo>
RelGroupCatalogEntry::getBoundExtraCreateInfo(transaction::Transaction*) const {
    std::vector<NodePair> nodePairs;
    for (auto& relTableInfo : relTableInfos) {
        nodePairs.push_back(relTableInfo.nodePair);
    }
    return std::make_unique<binder::BoundExtraCreateRelTableGroupInfo>(
        copyVector(propertyCollection.getDefinitions()), srcMultiplicity, dstMultiplicity,
        storageDirection, std::move(nodePairs));
}

} // namespace catalog
} // namespace kuzu
