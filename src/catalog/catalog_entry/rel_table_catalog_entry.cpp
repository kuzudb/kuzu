#include "catalog/catalog_entry/rel_table_catalog_entry.h"

#include <sstream>

#include "catalog/catalog.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

RelTableCatalogEntry::RelTableCatalogEntry(std::string name, table_id_t tableID,
    RelMultiplicity srcMultiplicity, RelMultiplicity dstMultiplicity, table_id_t srcTableID,
    table_id_t dstTableID)
    : TableCatalogEntry{CatalogEntryType::REL_TABLE_ENTRY, std::move(name), tableID},
      srcMultiplicity{srcMultiplicity}, dstMultiplicity{dstMultiplicity}, srcTableID{srcTableID},
      dstTableID{dstTableID} {}

RelTableCatalogEntry::RelTableCatalogEntry(const RelTableCatalogEntry& other)
    : TableCatalogEntry{other} {
    srcMultiplicity = other.srcMultiplicity;
    dstMultiplicity = other.dstMultiplicity;
    srcTableID = other.srcTableID;
    dstTableID = other.dstTableID;
}

bool RelTableCatalogEntry::isParent(table_id_t tableID) {
    return srcTableID == tableID || dstTableID == tableID;
}

column_id_t RelTableCatalogEntry::getColumnID(property_id_t propertyID) const {
    auto it = std::find_if(properties.begin(), properties.end(),
        [&propertyID](const auto& property) { return property.getPropertyID() == propertyID; });
    // Skip the first column in the rel table, which is reserved for nbrID.
    return it == properties.end() ? INVALID_COLUMN_ID : std::distance(properties.begin(), it) + 1;
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

std::unique_ptr<CatalogEntry> RelTableCatalogEntry::copy() const {
    return std::make_unique<RelTableCatalogEntry>(*this);
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

} // namespace catalog
} // namespace kuzu
