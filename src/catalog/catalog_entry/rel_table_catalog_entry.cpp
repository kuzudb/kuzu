#include "catalog/catalog_entry/rel_table_catalog_entry.h"

#include "catalog/catalog.h"

namespace kuzu {
namespace catalog {

RelTableCatalogEntry::RelTableCatalogEntry(std::string name, common::table_id_t tableID,
    common::RelMultiplicity srcMultiplicity, common::RelMultiplicity dstMultiplicity,
    common::table_id_t srcTableID, common::table_id_t dstTableID)
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

bool RelTableCatalogEntry::isParent(common::table_id_t tableID) {
    return srcTableID == tableID || dstTableID == tableID;
}

bool RelTableCatalogEntry::isSingleMultiplicity(common::RelDataDirection direction) const {
    return getMultiplicity(direction) == common::RelMultiplicity::ONE;
}
common::RelMultiplicity RelTableCatalogEntry::getMultiplicity(
    common::RelDataDirection direction) const {
    return direction == common::RelDataDirection::FWD ? dstMultiplicity : srcMultiplicity;
}
common::table_id_t RelTableCatalogEntry::getBoundTableID(
    common::RelDataDirection relDirection) const {
    return relDirection == common::RelDataDirection::FWD ? srcTableID : dstTableID;
}
common::table_id_t RelTableCatalogEntry::getNbrTableID(
    common::RelDataDirection relDirection) const {
    return relDirection == common::RelDataDirection::FWD ? dstTableID : srcTableID;
}

void RelTableCatalogEntry::serialize(common::Serializer& serializer) const {
    TableCatalogEntry::serialize(serializer);
    serializer.write(srcMultiplicity);
    serializer.write(dstMultiplicity);
    serializer.write(srcTableID);
    serializer.write(dstTableID);
}

std::unique_ptr<RelTableCatalogEntry> RelTableCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
    common::RelMultiplicity srcMultiplicity;
    common::RelMultiplicity dstMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
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
    ss << "CREATE REL TABLE " << getName() << "( FROM " << srcTableName << " TO " << dstTableName
       << ", ";
    Property::toCypher(getPropertiesRef(), ss);
    auto srcMultiStr = srcMultiplicity == common::RelMultiplicity::MANY ? "MANY" : "ONE";
    auto dstMultiStr = dstMultiplicity == common::RelMultiplicity::MANY ? "MANY" : "ONE";
    ss << srcMultiStr << "_" << dstMultiStr << ");";
    return ss.str();
}

} // namespace catalog
} // namespace kuzu
