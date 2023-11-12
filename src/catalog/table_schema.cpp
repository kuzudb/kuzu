#include "catalog/table_schema.h"

#include "catalog/node_table_schema.h"
#include "catalog/rdf_graph_schema.h"
#include "catalog/rel_table_group_schema.h"
#include "catalog/rel_table_schema.h"
#include "common/constants.h"
#include "common/exception/internal.h"
#include "common/exception/runtime.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/string_format.h"
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

bool TableSchema::containProperty(const std::string& propertyName) const {
    return std::any_of(properties.begin(), properties.end(),
        [&propertyName](const std::unique_ptr<Property>& property) {
            return property->getName() == propertyName;
        });
}

bool TableSchema::containsColumnType(const common::LogicalType& logicalType) const {
    return std::any_of(properties.begin(), properties.end(),
        [&logicalType](const std::unique_ptr<Property>& property) {
            return *property->getDataType() == logicalType;
        });
}

void TableSchema::dropProperty(common::property_id_t propertyID) {
    properties.erase(std::remove_if(properties.begin(), properties.end(),
                         [propertyID](const std::unique_ptr<Property>& property) {
                             return property->getPropertyID() == propertyID;
                         }),
        properties.end());
}

property_id_t TableSchema::getPropertyID(const std::string& propertyName) const {
    for (auto& property : properties) {
        if (property->getName() == propertyName) {
            return property->getPropertyID();
        }
    }
    // LCOV_EXCL_START
    throw RuntimeException(stringFormat(
        "Table: {} doesn't have a property with propertyName={}.", tableName, propertyName));
    // LCOV_EXCL_STOP
}

// TODO(Guodong): Instead of looping over properties, cache a map between propertyID and columnID.
column_id_t TableSchema::getColumnID(const property_id_t propertyID) const {
    for (auto i = 0u; i < properties.size(); i++) {
        if (properties[i]->getPropertyID() == propertyID) {
            return i;
        }
    }
    return INVALID_COLUMN_ID;
}

Property* TableSchema::getProperty(property_id_t propertyID) const {
    for (auto& property : properties) {
        if (property->getPropertyID() == propertyID) {
            return property.get();
        }
    }
    // LCOV_EXCL_START
    throw RuntimeException(stringFormat(
        "Table: {} doesn't have a property with propertyID={}.", tableName, propertyID));
    // LCOV_EXCL_STOP
}

void TableSchema::renameProperty(property_id_t propertyID, const std::string& newName) {
    for (auto& property : properties) {
        if (property->getPropertyID() == propertyID) {
            property->rename(newName);
            return;
        }
    }
    // LCOV_EXCL_START
    throw InternalException(stringFormat("Property with id={} not found.", propertyID));
    // LCOV_EXCL_STOP
}

void TableSchema::serialize(Serializer& serializer) {
    serializer.serializeValue(tableName);
    serializer.serializeValue(tableID);
    serializer.serializeValue(tableType);
    serializer.serializeVectorOfPtrs(properties);
    serializer.serializeValue(comment);
    serializer.serializeValue(nextPropertyID);
    serializeInternal(serializer);
}

std::unique_ptr<TableSchema> TableSchema::deserialize(Deserializer& deserializer) {
    std::string tableName;
    table_id_t tableID;
    TableType tableType;
    std::vector<std::unique_ptr<Property>> properties;
    std::string comment;
    property_id_t nextPropertyID;
    deserializer.deserializeValue(tableName);
    deserializer.deserializeValue(tableID);
    deserializer.deserializeValue(tableType);
    deserializer.deserializeVectorOfPtrs(properties);
    deserializer.deserializeValue(comment);
    deserializer.deserializeValue(nextPropertyID);
    std::unique_ptr<TableSchema> result;
    switch (tableType) {
    case TableType::NODE: {
        result = NodeTableSchema::deserialize(deserializer);
    } break;
    case TableType::REL: {
        result = RelTableSchema::deserialize(deserializer);
    } break;
    case TableType::REL_GROUP: {
        result = RelTableGroupSchema::deserialize(deserializer);
    } break;
    case TableType::RDF: {
        result = RdfGraphSchema::deserialize(deserializer);
    } break;
    default: {
        KU_UNREACHABLE;
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
