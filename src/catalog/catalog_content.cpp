#include "catalog/catalog_content.h"

#include "common/ser_deser.h"
#include "common/string_utils.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace catalog {

CatalogContent::CatalogContent() : nextTableID{0} {
    registerBuiltInFunctions();
}

CatalogContent::CatalogContent(const std::string& directory) {
    readFromFile(directory, DBFileType::ORIGINAL);
    registerBuiltInFunctions();
}

CatalogContent::CatalogContent(const CatalogContent& other) {
    for (auto& nodeTableSchema : other.nodeTableSchemas) {
        auto newNodeTableSchema = std::make_unique<NodeTableSchema>(*nodeTableSchema.second);
        nodeTableSchemas[newNodeTableSchema->tableID] = std::move(newNodeTableSchema);
    }
    for (auto& relTableSchema : other.relTableSchemas) {
        auto newRelTableSchema = std::make_unique<RelTableSchema>(*relTableSchema.second);
        relTableSchemas[newRelTableSchema->tableID] = std::move(newRelTableSchema);
    }
    nodeTableNameToIDMap = other.nodeTableNameToIDMap;
    relTableNameToIDMap = other.relTableNameToIDMap;
    nextTableID = other.nextTableID;
    registerBuiltInFunctions();
    for (auto& macro : other.macros) {
        macros.emplace(macro.first, macro.second->copy());
    }
}

table_id_t CatalogContent::addNodeTableSchema(
    std::string tableName, property_id_t primaryKeyId, std::vector<Property> properties) {
    table_id_t tableID = assignNextTableID();
    for (auto i = 0u; i < properties.size(); ++i) {
        properties[i].propertyID = i;
        properties[i].tableID = tableID;
    }
    auto nodeTableSchema = std::make_unique<NodeTableSchema>(
        std::move(tableName), tableID, primaryKeyId, std::move(properties));
    nodeTableNameToIDMap[nodeTableSchema->tableName] = tableID;
    nodeTableSchemas[tableID] = std::move(nodeTableSchema);
    return tableID;
}

table_id_t CatalogContent::addRelTableSchema(std::string tableName, RelMultiplicity relMultiplicity,
    std::vector<Property> properties, table_id_t srcTableID, table_id_t dstTableID,
    LogicalType srcPKDataType, LogicalType dstPKDataType) {
    table_id_t tableID = assignNextTableID();
    nodeTableSchemas[srcTableID]->addFwdRelTableID(tableID);
    nodeTableSchemas[dstTableID]->addBwdRelTableID(tableID);
    auto relInternalIDProperty =
        Property(InternalKeyword::ID, LogicalType{LogicalTypeID::INTERNAL_ID});
    properties.insert(properties.begin(), relInternalIDProperty);
    for (auto i = 0u; i < properties.size(); ++i) {
        properties[i].propertyID = i;
        properties[i].tableID = tableID;
    }
    auto relTableSchema = std::make_unique<RelTableSchema>(std::move(tableName), tableID,
        relMultiplicity, std::move(properties), srcTableID, dstTableID, std::move(srcPKDataType),
        std::move(dstPKDataType));
    relTableNameToIDMap[relTableSchema->tableName] = tableID;
    relTableSchemas[tableID] = std::move(relTableSchema);
    return tableID;
}

Property& CatalogContent::getNodeProperty(
    table_id_t tableID, const std::string& propertyName) const {
    for (auto& property : nodeTableSchemas.at(tableID)->properties) {
        if (propertyName == property.name) {
            return property;
        }
    }
    throw CatalogException("Cannot find node property " + propertyName + ".");
}

const Property& CatalogContent::getRelProperty(
    table_id_t tableID, const std::string& propertyName) const {
    for (auto& property : relTableSchemas.at(tableID)->properties) {
        if (propertyName == property.name) {
            return property;
        }
    }
    throw CatalogException("Cannot find rel property " + propertyName + ".");
}

void CatalogContent::dropTableSchema(table_id_t tableID) {
    auto tableSchema = getTableSchema(tableID);
    switch (tableSchema->tableType) {
    case TableType::NODE: {
        nodeTableNameToIDMap.erase(tableSchema->tableName);
        nodeTableSchemas.erase(tableID);
    } break;
    case TableType::REL: {
        relTableNameToIDMap.erase(tableSchema->tableName);
        relTableSchemas.erase(tableID);
    } break;
    default: {
        throw NotImplementedException("CatalogContent::dropTableSchema");
    }
    }
}

void CatalogContent::renameTable(table_id_t tableID, const std::string& newName) {
    auto tableSchema = getTableSchema(tableID);
    auto& tableNameToIDMap =
        tableSchema->tableType == TableType::NODE ? nodeTableNameToIDMap : relTableNameToIDMap;
    tableNameToIDMap.erase(tableSchema->tableName);
    tableNameToIDMap.emplace(newName, tableID);
    tableSchema->tableName = newName;
}

void CatalogContent::saveToFile(const std::string& directory, DBFileType dbFileType) {
    auto catalogPath = StorageUtils::getCatalogFilePath(directory, dbFileType);
    auto fileInfo = FileUtils::openFile(catalogPath, O_WRONLY | O_CREAT);
    uint64_t offset = 0;
    writeMagicBytes(fileInfo.get(), offset);
    SerDeser::serializeValue(StorageVersionInfo::getStorageVersion(), fileInfo.get(), offset);
    SerDeser::serializeValue(nodeTableSchemas.size(), fileInfo.get(), offset);
    SerDeser::serializeValue(relTableSchemas.size(), fileInfo.get(), offset);
    for (auto& [tableID, nodeTableSchema] : nodeTableSchemas) {
        SerDeser::serializeValue(tableID, fileInfo.get(), offset);
        nodeTableSchema->serialize(fileInfo.get(), offset);
    }
    for (auto& [tableID, relTableSchema] : relTableSchemas) {
        SerDeser::serializeValue(tableID, fileInfo.get(), offset);
        relTableSchema->serialize(fileInfo.get(), offset);
    }
    SerDeser::serializeValue(nextTableID, fileInfo.get(), offset);
    SerDeser::serializeUnorderedMap(macros, fileInfo.get(), offset);
}

void CatalogContent::readFromFile(const std::string& directory, DBFileType dbFileType) {
    auto catalogPath = StorageUtils::getCatalogFilePath(directory, dbFileType);
    auto fileInfo = FileUtils::openFile(catalogPath, O_RDONLY);
    uint64_t offset = 0;
    validateMagicBytes(fileInfo.get(), offset);
    storage_version_t savedStorageVersion;
    SerDeser::deserializeValue(savedStorageVersion, fileInfo.get(), offset);
    validateStorageVersion(savedStorageVersion);
    uint64_t numNodeTables, numRelTables;
    SerDeser::deserializeValue(numNodeTables, fileInfo.get(), offset);
    SerDeser::deserializeValue(numRelTables, fileInfo.get(), offset);
    table_id_t tableID;
    for (auto i = 0u; i < numNodeTables; i++) {
        SerDeser::deserializeValue(tableID, fileInfo.get(), offset);
        nodeTableSchemas[tableID] = ku_static_unique_pointer_cast<TableSchema, NodeTableSchema>(
            TableSchema::deserialize(fileInfo.get(), offset));
    }
    for (auto i = 0u; i < numRelTables; i++) {
        SerDeser::deserializeValue(tableID, fileInfo.get(), offset);
        relTableSchemas[tableID] = ku_static_unique_pointer_cast<TableSchema, RelTableSchema>(
            TableSchema::deserialize(fileInfo.get(), offset));
    }
    // Construct the tableNameToIdMap.
    for (auto& nodeTableSchema : nodeTableSchemas) {
        nodeTableNameToIDMap[nodeTableSchema.second->tableName] = nodeTableSchema.second->tableID;
    }
    for (auto& relTableSchema : relTableSchemas) {
        relTableNameToIDMap[relTableSchema.second->tableName] = relTableSchema.second->tableID;
    }
    SerDeser::deserializeValue(nextTableID, fileInfo.get(), offset);
    SerDeser::deserializeUnorderedMap(macros, fileInfo.get(), offset);
}

ExpressionType CatalogContent::getFunctionType(const std::string& name) const {
    auto upperCaseName = StringUtils::getUpper(name);
    if (builtInVectorFunctions->containsFunction(upperCaseName)) {
        return FUNCTION;
    } else if (builtInAggregateFunctions->containsFunction(upperCaseName)) {
        return AGGREGATE_FUNCTION;
    } else if (macros.contains(upperCaseName)) {
        return MACRO;
    } else {
        throw CatalogException(name + " function does not exist.");
    }
}

void CatalogContent::addVectorFunction(
    std::string name, function::vector_function_definitions definitions) {
    StringUtils::toUpper(name);
    builtInVectorFunctions->addFunction(std::move(name), std::move(definitions));
}

void CatalogContent::addScalarMacroFunction(
    std::string name, std::unique_ptr<function::ScalarMacroFunction> macro) {
    StringUtils::toUpper(name);
    macros.emplace(std::move(name), std::move(macro));
}

void CatalogContent::validateStorageVersion(storage_version_t savedStorageVersion) {
    auto storageVersion = StorageVersionInfo::getStorageVersion();
    if (savedStorageVersion != storageVersion) {
        throw common::RuntimeException(StringUtils::string_format(
            "Trying to read a database file with a different version. "
            "Database file version: {}, Current build storage version: {}",
            savedStorageVersion, storageVersion));
    }
}

void CatalogContent::validateMagicBytes(FileInfo* fileInfo, offset_t& offset) {
    auto numMagicBytes = strlen(StorageVersionInfo::MAGIC_BYTES);
    uint8_t magicBytes[4];
    for (auto i = 0u; i < numMagicBytes; i++) {
        SerDeser::deserializeValue<uint8_t>(magicBytes[i], fileInfo, offset);
    }
    if (memcmp(magicBytes, StorageVersionInfo::MAGIC_BYTES, numMagicBytes) != 0) {
        throw common::RuntimeException(
            "This is not a valid Kuzu database directory for the current version of Kuzu.");
    }
}

void CatalogContent::writeMagicBytes(FileInfo* fileInfo, offset_t& offset) {
    auto numMagicBytes = strlen(StorageVersionInfo::MAGIC_BYTES);
    for (auto i = 0u; i < numMagicBytes; i++) {
        SerDeser::serializeValue<uint8_t>(StorageVersionInfo::MAGIC_BYTES[i], fileInfo, offset);
    }
}

void CatalogContent::registerBuiltInFunctions() {
    builtInVectorFunctions = std::make_unique<function::BuiltInVectorFunctions>();
    builtInAggregateFunctions = std::make_unique<function::BuiltInAggregateFunctions>();
    builtInTableFunctions = std::make_unique<function::BuiltInTableFunctions>();
}

} // namespace catalog
} // namespace kuzu
