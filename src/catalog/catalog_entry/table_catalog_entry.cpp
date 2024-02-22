#include "catalog/catalog_entry/table_catalog_entry.h"

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"

namespace kuzu {
namespace catalog {

bool TableCatalogEntry::containProperty(const std::string& propertyName) const {
    return std::any_of(properties.begin(), properties.end(),
        [&propertyName](const auto& property) { return property.getName() == propertyName; });
}

common::property_id_t TableCatalogEntry::getPropertyID(const std::string& propertyName) const {
    auto it = std::find_if(properties.begin(), properties.end(),
        [&propertyName](const auto& property) { return property.getName() == propertyName; });
    KU_ASSERT(it != properties.end());
    return it->getPropertyID();
}

const Property* TableCatalogEntry::getProperty(common::property_id_t propertyID) const {
    auto it = std::find_if(properties.begin(), properties.end(),
        [&propertyID](const auto& property) { return property.getPropertyID() == propertyID; });
    KU_ASSERT(it != properties.end());
    return &(*it);
}

common::column_id_t TableCatalogEntry::getColumnID(const common::property_id_t propertyID) const {
    auto it = std::find_if(properties.begin(), properties.end(),
        [&propertyID](const auto& property) { return property.getPropertyID() == propertyID; });
    return it == properties.end() ? common::INVALID_COLUMN_ID :
                                    std::distance(properties.begin(), it);
}

bool TableCatalogEntry::containPropertyType(const common::LogicalType& logicalType) const {
    return std::any_of(
        properties.begin(), properties.end(), [&logicalType](const Property& property) {
            return *property.getDataType() == logicalType;
        });
}

void TableCatalogEntry::addProperty(
    std::string propertyName, std::unique_ptr<common::LogicalType> dataType) {
    properties.emplace_back(std::move(propertyName), std::move(dataType), nextPID++, tableID);
}

void TableCatalogEntry::dropProperty(common::property_id_t propertyID) {
    properties.erase(std::remove_if(properties.begin(), properties.end(),
                         [propertyID](const Property& property) {
                             return property.getPropertyID() == propertyID;
                         }),
        properties.end());
}

void TableCatalogEntry::renameProperty(
    common::property_id_t propertyID, const std::string& newName) {
    auto it = std::find_if(properties.begin(), properties.end(),
        [&propertyID](const auto& property) { return property.getPropertyID() == propertyID; });
    KU_ASSERT(it != properties.end());
    it->rename(newName);
}

void TableCatalogEntry::serialize(common::Serializer& serializer) const {
    CatalogEntry::serialize(serializer);
    serializer.write(tableID);
    serializer.serializeVector(properties);
    serializer.write(comment);
    serializer.write(nextPID);
}

std::unique_ptr<TableCatalogEntry> TableCatalogEntry::deserialize(
    common::Deserializer& deserializer, CatalogEntryType type) {
    common::table_id_t tableID;
    std::vector<Property> properties;
    std::string comment;
    common::property_id_t nextPID;
    deserializer.deserializeValue(tableID);
    deserializer.deserializeVector(properties);
    deserializer.deserializeValue(comment);
    deserializer.deserializeValue(nextPID);
    std::unique_ptr<TableCatalogEntry> result;
    switch (type) {
    case CatalogEntryType::NODE_TABLE_ENTRY:
        result = NodeTableCatalogEntry::deserialize(deserializer);
        break;
    case CatalogEntryType::REL_TABLE_ENTRY:
        result = RelTableCatalogEntry::deserialize(deserializer);
        break;
    case CatalogEntryType::REL_GROUP_ENTRY:
        result = RelGroupCatalogEntry::deserialize(deserializer);
        break;
    case CatalogEntryType::RDF_GRAPH_ENTRY:
        result = RDFGraphCatalogEntry::deserialize(deserializer);
        break;
    default:
        KU_UNREACHABLE;
    }
    result->tableID = tableID;
    result->properties = std::move(properties);
    result->comment = std::move(comment);
    result->nextPID = nextPID;
    return result;
}

} // namespace catalog
} // namespace kuzu
