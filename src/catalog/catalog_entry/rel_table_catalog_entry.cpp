#include "catalog/catalog_entry/rel_table_catalog_entry.h"

#include <sstream>

#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog.h"
#include "common/serializer/deserializer.h"
#include "main/client_context.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace catalog {

bool RelTableCatalogEntry::isParent(table_id_t tableID) {
    return srcTableID == tableID || dstTableID == tableID;
}

bool RelTableCatalogEntry::isSingleMultiplicity(RelDataDirection direction) const {
    return getMultiplicity(direction) == RelMultiplicity::ONE;
}

RelMultiplicity RelTableCatalogEntry::getMultiplicity(RelDataDirection direction) const {
    return direction == RelDataDirection::FWD ? dstMultiplicity : srcMultiplicity;
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
    table_id_t srcTableID = INVALID_TABLE_ID;
    table_id_t dstTableID = INVALID_TABLE_ID;
    deserializer.validateDebuggingInfo(debuggingInfo, "srcMultiplicity");
    deserializer.deserializeValue(srcMultiplicity);
    deserializer.validateDebuggingInfo(debuggingInfo, "dstMultiplicity");
    deserializer.deserializeValue(dstMultiplicity);
    deserializer.validateDebuggingInfo(debuggingInfo, "srcTableID");
    deserializer.deserializeValue(srcTableID);
    deserializer.validateDebuggingInfo(debuggingInfo, "dstTableID");
    deserializer.deserializeValue(dstTableID);
    auto relTableEntry = std::make_unique<RelTableCatalogEntry>();
    relTableEntry->srcMultiplicity = srcMultiplicity;
    relTableEntry->dstMultiplicity = dstMultiplicity;
    relTableEntry->srcTableID = srcTableID;
    relTableEntry->dstTableID = dstTableID;
    return relTableEntry;
}

std::unique_ptr<TableCatalogEntry> RelTableCatalogEntry::copy() const {
    auto other = std::make_unique<RelTableCatalogEntry>();
    other->srcMultiplicity = srcMultiplicity;
    other->dstMultiplicity = dstMultiplicity;
    other->srcTableID = srcTableID;
    other->dstTableID = dstTableID;
    other->copyFrom(*this);
    return other;
}

std::string RelTableCatalogEntry::toCypher(main::ClientContext* clientContext) const {
    std::stringstream ss;
    auto catalog = clientContext->getCatalog();
    auto srcTableName = catalog->getTableName(clientContext->getTx(), srcTableID);
    auto dstTableName = catalog->getTableName(clientContext->getTx(), dstTableID);
    auto srcMultiStr = srcMultiplicity == common::RelMultiplicity::MANY ? "MANY" : "ONE";
    auto dstMultiStr = dstMultiplicity == common::RelMultiplicity::MANY ? "MANY" : "ONE";
    std::string tableInfo =
        stringFormat("CREATE REL TABLE {} (FROM {} TO {}, ", getName(), srcTableName, dstTableName);
    ss << tableInfo << propertyCollection.toCypher() << srcMultiStr << "_" << dstMultiStr << ");";
    return ss.str();
}

std::unique_ptr<BoundExtraCreateCatalogEntryInfo> RelTableCatalogEntry::getBoundExtraCreateInfo(
    transaction::Transaction*) const {
    return std::make_unique<binder::BoundExtraCreateRelTableInfo>(srcMultiplicity, dstMultiplicity,
        srcTableID, dstTableID, copyVector(propertyCollection.getDefinitions()));
}

} // namespace catalog
} // namespace kuzu
