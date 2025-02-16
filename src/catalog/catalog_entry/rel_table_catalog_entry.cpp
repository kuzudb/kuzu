#include "catalog/catalog_entry/rel_table_catalog_entry.h"

#include <sstream>

#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "common/serializer/deserializer.h"
#include "main/client_context.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace catalog {

bool RelTableCatalogEntry::isParent(table_id_t tableID) {
    return srcTableID == tableID || dstTableID == tableID;
}

bool RelTableCatalogEntry::hasParentRelGroup(const Catalog* catalog,
    const transaction::Transaction* transaction) const {
    return getParentRelGroup(catalog, transaction) != nullptr;
}

RelGroupCatalogEntry* RelTableCatalogEntry::getParentRelGroup(const Catalog* catalog,
    const transaction::Transaction* transaction) const {
    for (auto& relGroup : catalog->getRelGroupEntries(transaction)) {
        if (relGroup->isParent(getTableID())) {
            return relGroup;
        }
    }
    return nullptr;
}

bool RelTableCatalogEntry::isSingleMultiplicity(RelDataDirection direction) const {
    return getMultiplicity(direction) == RelMultiplicity::ONE;
}

RelMultiplicity RelTableCatalogEntry::getMultiplicity(RelDataDirection direction) const {
    return direction == RelDataDirection::FWD ? dstMultiplicity : srcMultiplicity;
}

std::vector<RelDataDirection> RelTableCatalogEntry::getRelDataDirections() const {
    switch (storageDirection) {
    case ExtendDirection::FWD: {
        return {RelDataDirection::FWD};
    }
    case ExtendDirection::BWD: {
        return {RelDataDirection::BWD};
    }
    case ExtendDirection::BOTH: {
        return {RelDataDirection::FWD, RelDataDirection::BWD};
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

table_id_t RelTableCatalogEntry::getBoundTableID(RelDataDirection relDirection) const {
    return relDirection == RelDataDirection::FWD ? srcTableID : dstTableID;
}

table_id_t RelTableCatalogEntry::getNbrTableID(RelDataDirection relDirection) const {
    return relDirection == RelDataDirection::FWD ? dstTableID : srcTableID;
}

void RelTableCatalogEntry::serialize(Serializer& serializer) const {
    TableCatalogEntry::serialize(serializer);
    serializer.writeDebuggingInfo("srcMultiplicity");
    serializer.write(srcMultiplicity);
    serializer.writeDebuggingInfo("dstMultiplicity");
    serializer.write(dstMultiplicity);
    serializer.writeDebuggingInfo("storageDirection");
    serializer.write(storageDirection);
    serializer.writeDebuggingInfo("srcTableID");
    serializer.write(srcTableID);
    serializer.writeDebuggingInfo("dstTableID");
    serializer.write(dstTableID);
}

std::unique_ptr<RelTableCatalogEntry> RelTableCatalogEntry::deserialize(
    Deserializer& deserializer) {
    std::string debuggingInfo;
    RelMultiplicity srcMultiplicity{};
    RelMultiplicity dstMultiplicity{};
    ExtendDirection storageDirection{};
    table_id_t srcTableID = INVALID_TABLE_ID;
    table_id_t dstTableID = INVALID_TABLE_ID;
    deserializer.validateDebuggingInfo(debuggingInfo, "srcMultiplicity");
    deserializer.deserializeValue(srcMultiplicity);
    deserializer.validateDebuggingInfo(debuggingInfo, "dstMultiplicity");
    deserializer.deserializeValue(dstMultiplicity);
    deserializer.validateDebuggingInfo(debuggingInfo, "storageDirection");
    deserializer.deserializeValue(storageDirection);
    deserializer.validateDebuggingInfo(debuggingInfo, "srcTableID");
    deserializer.deserializeValue(srcTableID);
    deserializer.validateDebuggingInfo(debuggingInfo, "dstTableID");
    deserializer.deserializeValue(dstTableID);
    auto relTableEntry = std::make_unique<RelTableCatalogEntry>();
    relTableEntry->srcMultiplicity = srcMultiplicity;
    relTableEntry->dstMultiplicity = dstMultiplicity;
    relTableEntry->storageDirection = storageDirection;
    relTableEntry->srcTableID = srcTableID;
    relTableEntry->dstTableID = dstTableID;
    return relTableEntry;
}

std::unique_ptr<TableCatalogEntry> RelTableCatalogEntry::copy() const {
    auto other = std::make_unique<RelTableCatalogEntry>();
    other->srcMultiplicity = srcMultiplicity;
    other->dstMultiplicity = dstMultiplicity;
    other->storageDirection = storageDirection;
    other->srcTableID = srcTableID;
    other->dstTableID = dstTableID;
    other->copyFrom(*this);
    return other;
}

std::string RelTableCatalogEntry::getMultiplicityStr() const {
    return RelMultiplicityUtils::toString(srcMultiplicity) + "_" +
           RelMultiplicityUtils::toString(dstMultiplicity);
}

std::string RelTableCatalogEntry::toCypher(const ToCypherInfo& info) const {
    auto& relTableToCypherInfo = info.constCast<RelTableToCypherInfo>();
    auto clientContext = relTableToCypherInfo.context;
    std::stringstream ss;
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTransaction();
    auto srcTableName = catalog->getTableCatalogEntry(transaction, srcTableID)->getName();
    auto dstTableName = catalog->getTableCatalogEntry(transaction, dstTableID)->getName();
    std::string tableInfo = stringFormat("CREATE REL TABLE `{}` (FROM {} TO {}, ", getName(),
        srcTableName, dstTableName);
    ss << tableInfo << propertiesToCypher() << getMultiplicityStr() << ");";
    return ss.str();
}

ExtendDirection RelTableCatalogEntry::getStorageDirection() const {
    return storageDirection;
}

std::unique_ptr<BoundExtraCreateCatalogEntryInfo> RelTableCatalogEntry::getBoundExtraCreateInfo(
    transaction::Transaction*) const {
    return std::make_unique<BoundExtraCreateRelTableInfo>(srcMultiplicity, dstMultiplicity,
        storageDirection, srcTableID, dstTableID, copyVector(propertyCollection.getDefinitions()));
}

} // namespace catalog
} // namespace kuzu
