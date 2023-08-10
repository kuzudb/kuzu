#include "catalog/table_schema.h"

#include "common/exception.h"
#include "common/ser_deser.h"
#include "common/string_utils.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace catalog {

RelMultiplicity getRelMultiplicityFromString(const std::string& relMultiplicityString) {
    if ("ONE_ONE" == relMultiplicityString) {
        return RelMultiplicity::ONE_ONE;
    } else if ("MANY_ONE" == relMultiplicityString) {
        return RelMultiplicity::MANY_ONE;
    } else if ("ONE_MANY" == relMultiplicityString) {
        return RelMultiplicity::ONE_MANY;
    } else if ("MANY_MANY" == relMultiplicityString) {
        return RelMultiplicity::MANY_MANY;
    }
    throw CatalogException("Invalid relMultiplicity string '" + relMultiplicityString + "'.");
}

std::string getRelMultiplicityAsString(RelMultiplicity relMultiplicity) {
    switch (relMultiplicity) {
    case RelMultiplicity::MANY_MANY: {
        return "MANY_MANY";
    }
    case RelMultiplicity::MANY_ONE: {
        return "MANY_ONE";
    }
    case RelMultiplicity::ONE_ONE: {
        return "ONE_ONE";
    }
    case RelMultiplicity::ONE_MANY: {
        return "ONE_MANY";
    }
    default:
        throw CatalogException("Cannot convert rel multiplicity to std::string.");
    }
}

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
    SerDeser::serializeValue(nextPropertyID, fileInfo, offset);
    serializeInternal(fileInfo, offset);
}

std::unique_ptr<TableSchema> TableSchema::deserialize(FileInfo* fileInfo, uint64_t& offset) {
    std::string tableName;
    table_id_t tableID;
    TableType tableType;
    std::vector<std::unique_ptr<Property>> properties;
    property_id_t nextPropertyID;
    SerDeser::deserializeValue(tableName, fileInfo, offset);
    SerDeser::deserializeValue(tableID, fileInfo, offset);
    SerDeser::deserializeValue(tableType, fileInfo, offset);
    SerDeser::deserializeVectorOfPtrs(properties, fileInfo, offset);
    SerDeser::deserializeValue(nextPropertyID, fileInfo, offset);
    std::unique_ptr<TableSchema> result;
    switch (tableType) {
    case TableType::NODE: {
        result = NodeTableSchema::deserialize(fileInfo, offset);
    } break;
    case TableType::REL: {
        result = RelTableSchema::deserialize(fileInfo, offset);
    } break;
    default: {
        throw NotImplementedException{"TableSchema::deserialize"};
    }
    }
    result->tableName = tableName;
    result->tableID = tableID;
    result->tableType = tableType;
    result->properties = std::move(properties);
    result->nextPropertyID = nextPropertyID;
    return result;
}

std::vector<std::unique_ptr<Property>> TableSchema::copyProperties() const {
    std::vector<std::unique_ptr<Property>> propertiesCopy;
    propertiesCopy.reserve(properties.size());
    for (auto& property : properties) {
        propertiesCopy.push_back(property->copy());
    }
    return propertiesCopy;
}

void NodeTableSchema::serializeInternal(FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::serializeValue(primaryKeyPropertyID, fileInfo, offset);
    SerDeser::serializeUnorderedSet(fwdRelTableIDSet, fileInfo, offset);
    SerDeser::serializeUnorderedSet(bwdRelTableIDSet, fileInfo, offset);
}

std::unique_ptr<NodeTableSchema> NodeTableSchema::deserialize(
    FileInfo* fileInfo, uint64_t& offset) {
    property_id_t primaryKeyPropertyID;
    std::unordered_set<table_id_t> fwdRelTableIDSet;
    std::unordered_set<table_id_t> bwdRelTableIDSet;
    SerDeser::deserializeValue(primaryKeyPropertyID, fileInfo, offset);
    SerDeser::deserializeUnorderedSet(fwdRelTableIDSet, fileInfo, offset);
    SerDeser::deserializeUnorderedSet(bwdRelTableIDSet, fileInfo, offset);
    return std::make_unique<NodeTableSchema>(
        primaryKeyPropertyID, fwdRelTableIDSet, bwdRelTableIDSet);
}

void RelTableSchema::serializeInternal(FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::serializeValue(relMultiplicity, fileInfo, offset);
    SerDeser::serializeValue(srcTableID, fileInfo, offset);
    SerDeser::serializeValue(dstTableID, fileInfo, offset);
    srcPKDataType->serialize(fileInfo, offset);
    dstPKDataType->serialize(fileInfo, offset);
}

std::unique_ptr<RelTableSchema> RelTableSchema::deserialize(FileInfo* fileInfo, uint64_t& offset) {
    RelMultiplicity relMultiplicity;
    table_id_t srcTableID;
    table_id_t dstTableID;
    SerDeser::deserializeValue(relMultiplicity, fileInfo, offset);
    SerDeser::deserializeValue(srcTableID, fileInfo, offset);
    SerDeser::deserializeValue(dstTableID, fileInfo, offset);
    auto srcPKDataType = LogicalType::deserialize(fileInfo, offset);
    auto dstPKDataType = LogicalType::deserialize(fileInfo, offset);
    return std::make_unique<RelTableSchema>(relMultiplicity, srcTableID, dstTableID,
        std::move(srcPKDataType), std::move(dstPKDataType));
}

} // namespace catalog
} // namespace kuzu
