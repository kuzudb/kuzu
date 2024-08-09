#include "catalog/catalog_entry/table_catalog_entry.h"

#include "binder/ddl/bound_alter_info.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/serializer/deserializer.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace catalog {

std::unique_ptr<TableCatalogEntry> TableCatalogEntry::alter(const BoundAlterInfo& info) {
    KU_ASSERT(!deleted);
    auto newEntry = copy();
    switch (info.alterType) {
    case AlterType::RENAME_TABLE: {
        auto& renameTableInfo = *info.extraInfo->constPtrCast<BoundExtraRenameTableInfo>();
        newEntry->rename(renameTableInfo.newName);
    } break;
    case AlterType::RENAME_PROPERTY: {
        auto& renamePropInfo = *info.extraInfo->constPtrCast<BoundExtraRenamePropertyInfo>();
        newEntry->renameProperty(renamePropInfo.oldName, renamePropInfo.newName);
    } break;
    case AlterType::ADD_PROPERTY: {
        auto& addPropInfo = *info.extraInfo->constPtrCast<BoundExtraAddPropertyInfo>();
        newEntry->addProperty(addPropInfo.propertyDefinition);
    } break;
    case AlterType::DROP_PROPERTY: {
        auto& dropPropInfo = *info.extraInfo->constPtrCast<BoundExtraDropPropertyInfo>();
        newEntry->dropProperty(dropPropInfo.propertyName);
    } break;
    case AlterType::COMMENT: {
        auto& commentInfo = *info.extraInfo->constPtrCast<BoundExtraCommentInfo>();
        newEntry->setComment(commentInfo.comment);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return newEntry;
}

common::column_id_t TableCatalogEntry::getMaxColumnID() const {
    return propertyCollection.getMaxColumnID();
}

std::string TableCatalogEntry::propertiesToCypher() const {
    return propertyCollection.toCypher();
}

bool TableCatalogEntry::containsProperty(const std::string& propertyName) const {
    return propertyCollection.contains(propertyName);
}

common::idx_t TableCatalogEntry::getPropertyIdx(const std::string& propertyName) const {
    return propertyCollection.getIdx(propertyName);
}

const PropertyDefinition& TableCatalogEntry::getProperty(const std::string& propertyName) const {
    return propertyCollection.getDefinition(propertyName);
}

const PropertyDefinition& TableCatalogEntry::getProperty(common::idx_t idx) const {
    return propertyCollection.getDefinition(idx);
}

common::column_id_t TableCatalogEntry::getColumnID(const std::string& propertyName) const {
    return propertyCollection.getColumnID(propertyName);
}

void TableCatalogEntry::addProperty(const PropertyDefinition& propertyDefinition) {
    propertyCollection.add(propertyDefinition);
}

void TableCatalogEntry::dropProperty(const std::string& propertyName) {
    propertyCollection.drop(propertyName);
}

void TableCatalogEntry::renameProperty(const std::string& propertyName,
    const std::string& newName) {
    propertyCollection.rename(propertyName, newName);
}

void TableCatalogEntry::serialize(common::Serializer& serializer) const {
    CatalogEntry::serialize(serializer);
    serializer.write(tableID);
    serializer.write(comment);
    propertyCollection.serialize(serializer);
}

std::unique_ptr<TableCatalogEntry> TableCatalogEntry::deserialize(
    common::Deserializer& deserializer, CatalogEntryType type) {
    common::table_id_t tableID;
    std::string comment;
    deserializer.deserializeValue(tableID);
    deserializer.deserializeValue(comment);
    auto propertyCollection = PropertyDefinitionCollection::deserialize(deserializer);
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
    result->comment = std::move(comment);
    result->propertyCollection = std::move(propertyCollection);
    return result;
}

void TableCatalogEntry::copyFrom(const CatalogEntry& other) {
    CatalogEntry::copyFrom(other);
    auto& otherTable = ku_dynamic_cast<const CatalogEntry&, const TableCatalogEntry&>(other);
    set = otherTable.set;
    tableID = otherTable.tableID;
    comment = otherTable.comment;
    propertyCollection = otherTable.propertyCollection.copy();
}

binder::BoundCreateTableInfo TableCatalogEntry::getBoundCreateTableInfo(
    transaction::Transaction* transaction) const {
    auto extraInfo = getBoundExtraCreateInfo(transaction);
    return BoundCreateTableInfo(getTableType(), name, common::ConflictAction::ON_CONFLICT_THROW,
        std::move(extraInfo));
}

} // namespace catalog
} // namespace kuzu
