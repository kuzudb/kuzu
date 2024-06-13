#include "catalog/catalog_entry/table_catalog_entry.h"

#include <algorithm>

#include "binder/ddl/bound_alter_info.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace catalog {

std::unique_ptr<TableCatalogEntry> TableCatalogEntry::alter(const BoundAlterInfo& alterInfo) {
    KU_ASSERT(!deleted);
    auto newEntry = copy();
    switch (alterInfo.alterType) {
    case AlterType::RENAME_TABLE: {
        auto& renameTableInfo = *alterInfo.extraInfo->constPtrCast<BoundExtraRenameTableInfo>();
        newEntry->rename(renameTableInfo.newName);
    } break;
    case AlterType::RENAME_PROPERTY: {
        auto& renamePropInfo = *alterInfo.extraInfo->constPtrCast<BoundExtraRenamePropertyInfo>();
        newEntry->renameProperty(renamePropInfo.propertyID, renamePropInfo.newName);
    } break;
    case AlterType::ADD_PROPERTY: {
        auto& addPropInfo = *alterInfo.extraInfo->constPtrCast<BoundExtraAddPropertyInfo>();
        newEntry->addProperty(addPropInfo.propertyName, addPropInfo.dataType.copy(),
            addPropInfo.defaultValue->copy());
    } break;
    case AlterType::DROP_PROPERTY: {
        auto& dropPropInfo = *alterInfo.extraInfo->constPtrCast<BoundExtraDropPropertyInfo>();
        newEntry->dropProperty(dropPropInfo.propertyID);
    } break;
    case AlterType::COMMENT: {
        auto& commentInfo = *alterInfo.extraInfo->constPtrCast<BoundExtraCommentInfo>();
        newEntry->setComment(commentInfo.comment);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return newEntry;
}

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

uint32_t TableCatalogEntry::getPropertyPos(common::property_id_t propertyID) const {
    auto it = std::find_if(properties.begin(), properties.end(),
        [&propertyID](const auto& property) { return property.getPropertyID() == propertyID; });
    KU_ASSERT(it != properties.end());
    return it - properties.begin();
}

common::column_id_t TableCatalogEntry::getColumnID(const common::property_id_t propertyID) const {
    auto it = std::find_if(properties.begin(), properties.end(),
        [&propertyID](const auto& property) { return property.getPropertyID() == propertyID; });
    KU_ASSERT(it != properties.end());
    return it->getColumnID();
}

bool TableCatalogEntry::containPropertyType(const common::LogicalType& logicalType) const {
    return std::any_of(properties.begin(), properties.end(),
        [&logicalType](const Property& property) { return property.getDataType() == logicalType; });
}

void TableCatalogEntry::addProperty(std::string propertyName, common::LogicalType dataType,
    std::unique_ptr<parser::ParsedExpression> defaultExpr) {
    properties.emplace_back(std::move(propertyName), std::move(dataType), std::move(defaultExpr),
        nextPID++, nextColumnID++, tableID);
}

void TableCatalogEntry::dropProperty(common::property_id_t propertyID) {
    properties.erase(std::remove_if(properties.begin(), properties.end(),
                         [propertyID](const Property& property) {
                             return property.getPropertyID() == propertyID;
                         }),
        properties.end());
}

void TableCatalogEntry::renameProperty(common::property_id_t propertyID,
    const std::string& newName) {
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
    serializer.write(nextColumnID);
}

std::unique_ptr<TableCatalogEntry> TableCatalogEntry::deserialize(
    common::Deserializer& deserializer, CatalogEntryType type) {
    common::table_id_t tableID;
    std::vector<Property> properties;
    std::string comment;
    common::property_id_t nextPID;
    common::column_id_t nextColumnID;
    deserializer.deserializeValue(tableID);
    deserializer.deserializeVector(properties);
    deserializer.deserializeValue(comment);
    deserializer.deserializeValue(nextPID);
    deserializer.deserializeValue(nextColumnID);
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
    result->nextColumnID = nextColumnID;
    return result;
}

void TableCatalogEntry::copyFrom(const CatalogEntry& other) {
    CatalogEntry::copyFrom(other);
    auto& otherTable = ku_dynamic_cast<const CatalogEntry&, const TableCatalogEntry&>(other);
    tableID = otherTable.tableID;
    comment = otherTable.comment;
    nextPID = otherTable.nextPID;
    nextColumnID = otherTable.nextColumnID;
    properties = copyVector(otherTable.properties);
}

binder::BoundCreateTableInfo TableCatalogEntry::getBoundCreateTableInfo(
    transaction::Transaction* transaction) const {
    auto extraInfo = getBoundExtraCreateInfo(transaction);
    return BoundCreateTableInfo(getTableType(), name, common::ConflictAction::ON_CONFLICT_THROW,
        std::move(extraInfo));
}

} // namespace catalog
} // namespace kuzu
