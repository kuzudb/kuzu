#include "catalog/table_schema.h"

#include "catalog/node_table_schema.h"
#include "catalog/rdf_graph_schema.h"
#include "catalog/rel_table_group_schema.h"
#include "catalog/rel_table_schema.h"
#include "common/constants.h"
#include "common/exception/catalog.h"
#include "common/exception/internal.h"
#include "common/exception/not_implemented.h"
#include "common/exception/runtime.h"
#include "common/ser_deser.h"
#include "common/string_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

bool TableSchema::isReservedPropertyName(const std::string& propertyName) {
    return StringUtils::getUpper(propertyName) == InternalKeyword::ID;
}

std::vector<Property*> TableSchema::getProperties() const {
    std::vector<Property*> propertiesToReturn;
    propertiesToReturn.reserve(properties.size());
    for (auto& property : properties) {
        propertiesToReturn.push_back(property.get());
    }
    return propertiesToReturn;
}

std::string TableSchema::getPropertyName(property_id_t propertyID) const {
    for (auto& property : properties) {
        if (property->getPropertyID() == propertyID) {
            return property->getName();
        }
    }
    throw RuntimeException(StringUtils::string_format(
        "Table: {} doesn't have a property with propertyID={}.", tableName, propertyID));
}

property_id_t TableSchema::getPropertyID(const std::string& propertyName) const {
    for (auto& property : properties) {
        if (property->getName() == propertyName) {
            return property->getPropertyID();
        }
    }
    throw RuntimeException(StringUtils::string_format(
        "Table: {} doesn't have a property with propertyName={}.", tableName, propertyName));
}

Property* TableSchema::getProperty(property_id_t propertyID) const {
    for (auto& property : properties) {
        if (property->getPropertyID() == propertyID) {
            return property.get();
        }
    }
    throw RuntimeException(StringUtils::string_format(
        "Table: {} doesn't have a property with propertyID={}.", tableName, propertyID));
}

void TableSchema::renameProperty(property_id_t propertyID, const std::string& newName) {
    for (auto& property : properties) {
        if (property->getPropertyID() == propertyID) {
            property->rename(newName);
            return;
        }
    }
    throw InternalException(
        StringUtils::string_format("Property with id={} not found.", propertyID));
}

void TableSchema::serialize(FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::serializeValue(tableName, fileInfo, offset);
    SerDeser::serializeValue(tableID, fileInfo, offset);
    SerDeser::serializeValue(tableType, fileInfo, offset);
    SerDeser::serializeVectorOfPtrs(properties, fileInfo, offset);
    SerDeser::serializeValue(comment, fileInfo, offset);
    SerDeser::serializeValue(nextPropertyID, fileInfo, offset);
    serializeInternal(fileInfo, offset);
}

std::unique_ptr<TableSchema> TableSchema::deserialize(FileInfo* fileInfo, uint64_t& offset) {
    std::string tableName;
    table_id_t tableID;
    TableType tableType;
    std::vector<std::unique_ptr<Property>> properties;
    std::string comment;
    property_id_t nextPropertyID;
    SerDeser::deserializeValue(tableName, fileInfo, offset);
    SerDeser::deserializeValue(tableID, fileInfo, offset);
    SerDeser::deserializeValue(tableType, fileInfo, offset);
    SerDeser::deserializeVectorOfPtrs(properties, fileInfo, offset);
    SerDeser::deserializeValue(comment, fileInfo, offset);
    SerDeser::deserializeValue(nextPropertyID, fileInfo, offset);
    std::unique_ptr<TableSchema> result;
    switch (tableType) {
    case TableType::NODE: {
        result = NodeTableSchema::deserialize(fileInfo, offset);
    } break;
    case TableType::REL: {
        result = RelTableSchema::deserialize(fileInfo, offset);
    } break;
    case TableType::REL_GROUP: {
        result = RelTableGroupSchema::deserialize(fileInfo, offset);
    } break;
    case TableType::RDF: {
        result = RdfGraphSchema::deserialize(fileInfo, offset);
    } break;
    default: {
        throw NotImplementedException{"TableSchema::deserialize"};
    }
    }
    result->tableName = tableName;
    result->tableID = tableID;
    result->tableType = tableType;
    result->properties = std::move(properties);
    result->comment = std::move(comment);
    result->nextPropertyID = nextPropertyID;
    return result;
}

} // namespace catalog
} // namespace kuzu
