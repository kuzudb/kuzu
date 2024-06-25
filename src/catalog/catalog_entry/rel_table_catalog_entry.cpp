#include "catalog/catalog_entry/rel_table_catalog_entry.h"

#include <sstream>

#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog.h"
#include "main/client_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

RelTableCatalogEntry::RelTableCatalogEntry(CatalogSet* set, std::string name, table_id_t tableID,
    RelMultiplicity srcMultiplicity, RelMultiplicity dstMultiplicity, table_id_t srcTableID,
    table_id_t dstTableID)
    : TableCatalogEntry{set, CatalogEntryType::REL_TABLE_ENTRY, std::move(name), tableID},
      srcMultiplicity{srcMultiplicity}, dstMultiplicity{dstMultiplicity}, srcTableID{srcTableID},
      dstTableID{dstTableID} {}

bool RelTableCatalogEntry::isParent(table_id_t tableID) {
    return srcTableID == tableID || dstTableID == tableID;
}

column_id_t RelTableCatalogEntry::getColumnID(property_id_t propertyID) const {
    auto it = std::find_if(properties.begin(), properties.end(),
        [&propertyID](const auto& property) { return property.getPropertyID() == propertyID; });
    // Skip the first column in the rel table, which is reserved for nbrID.
    return it == properties.end() ? INVALID_COLUMN_ID : it->getColumnID() + 1;
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
    serializer.write(srcMultiplicity);
    serializer.write(dstMultiplicity);
    serializer.write(srcTableID);
    serializer.write(dstTableID);
}

std::unique_ptr<RelTableCatalogEntry> RelTableCatalogEntry::deserialize(
    Deserializer& deserializer) {
    RelMultiplicity srcMultiplicity;
    RelMultiplicity dstMultiplicity;
    table_id_t srcTableID;
    table_id_t dstTableID;
    deserializer.deserializeValue(srcMultiplicity);
    deserializer.deserializeValue(dstMultiplicity);
    deserializer.deserializeValue(srcTableID);
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
    ss << tableInfo << Property::toCypher(getPropertiesRef()) << srcMultiStr << "_" << dstMultiStr
       << ");";
    return ss.str();
}

std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo>
RelTableCatalogEntry::getBoundExtraCreateInfo(transaction::Transaction*) const {
    std::vector<binder::PropertyInfo> propertyInfos;
    for (const auto& property : properties) {
        propertyInfos.emplace_back(property.getName(), property.getDataType().copy(),
            property.getDefaultExpr()->copy());
    }
    auto boundExtraCreateInfo = std::make_unique<binder::BoundExtraCreateRelTableInfo>(
        srcMultiplicity, dstMultiplicity, srcTableID, dstTableID, std::move(propertyInfos));
    return boundExtraCreateInfo;
}

} // namespace catalog
} // namespace kuzu
