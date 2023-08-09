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

table_id_t CatalogContent::addNodeTableSchema(std::string tableName, property_id_t primaryKeyId,
    std::vector<std::unique_ptr<Property>> properties) {
    table_id_t tableID = assignNextTableID();
    for (auto i = 0u; i < properties.size(); ++i) {
        properties[i]->setPropertyID(i);
        properties[i]->setTableID(tableID);
    }
    auto nodeTableSchema = std::make_unique<NodeTableSchema>(
        std::move(tableName), tableID, primaryKeyId, std::move(properties));
    tableNameToIDMap.emplace(nodeTableSchema->tableName, tableID);
    tableSchemas.emplace(tableID, std::move(nodeTableSchema));
    return tableID;
}

table_id_t CatalogContent::addRelTableSchema(std::string tableName, RelMultiplicity relMultiplicity,
    std::vector<std::unique_ptr<Property>> properties, table_id_t srcTableID, table_id_t dstTableID,
    std::unique_ptr<LogicalType> srcPKDataType, std::unique_ptr<LogicalType> dstPKDataType) {
    table_id_t tableID = assignNextTableID();
    getNodeTableSchema(srcTableID)->addFwdRelTableID(tableID);
    getNodeTableSchema(dstTableID)->addBwdRelTableID(tableID);
    auto relInternalIDProperty = std::make_unique<Property>(
        InternalKeyword::ID, std::make_unique<LogicalType>(LogicalTypeID::INTERNAL_ID));
    properties.insert(properties.begin(), std::move(relInternalIDProperty));
    for (auto i = 0u; i < properties.size(); ++i) {
        properties[i]->setPropertyID(i);
        properties[i]->setTableID(tableID);
    }
    auto relTableSchema = std::make_unique<RelTableSchema>(std::move(tableName), tableID,
        relMultiplicity, std::move(properties), srcTableID, dstTableID, std::move(srcPKDataType),
        std::move(dstPKDataType));
    tableNameToIDMap.emplace(relTableSchema->tableName, tableID);
    tableSchemas.emplace(tableID, std::move(relTableSchema));
    return tableID;
}

bool CatalogContent::containNodeTable(const std::string& tableName) const {
    if (!tableNameToIDMap.contains(tableName)) {
        return false;
    }
    auto tableID = getTableID(tableName);
    return tableSchemas.at(tableID)->tableType == TableType::NODE;
}

bool CatalogContent::containRelTable(const std::string& tableName) const {
    if (!tableNameToIDMap.contains(tableName)) {
        return false;
    }
    auto tableID = getTableID(tableName);
    return tableSchemas.at(tableID)->tableType == TableType::REL;
}

Property* CatalogContent::getNodeProperty(
    table_id_t tableID, const std::string& propertyName) const {
    for (auto& property : tableSchemas.at(tableID)->properties) {
        if (propertyName == property->getName()) {
            return property.get();
        }
    }
    throw CatalogException("Cannot find node property " + propertyName + ".");
}

Property* CatalogContent::getRelProperty(
    table_id_t tableID, const std::string& propertyName) const {
    for (auto& property : tableSchemas.at(tableID)->properties) {
        if (propertyName == property->getName()) {
            return property.get();
        }
    }
    throw CatalogException("Cannot find rel property " + propertyName + ".");
}

std::vector<NodeTableSchema*> CatalogContent::getNodeTableSchemas() const {
    std::vector<NodeTableSchema*> nodeTableSchemas;
    for (auto& tableSchema : tableSchemas) {
        if (tableSchema.second->getTableType() == TableType::NODE) {
            nodeTableSchemas.push_back(
                reinterpret_cast<NodeTableSchema*>(tableSchema.second.get()));
        }
    }
    return nodeTableSchemas;
}

std::vector<RelTableSchema*> CatalogContent::getRelTableSchemas() const {
    std::vector<RelTableSchema*> relTableSchemas;
    for (auto& tableSchema : tableSchemas) {
        if (tableSchema.second->getTableType() == TableType::REL) {
            relTableSchemas.push_back(reinterpret_cast<RelTableSchema*>(tableSchema.second.get()));
        }
    }
    return relTableSchemas;
}

void CatalogContent::dropTableSchema(table_id_t tableID) {
    auto tableName = getTableName(tableID);
    tableNameToIDMap.erase(tableName);
    tableSchemas.erase(tableID);
}

void CatalogContent::renameTable(table_id_t tableID, const std::string& newName) {
    auto tableSchema = getTableSchema(tableID);
    tableNameToIDMap.erase(tableSchema->tableName);
    tableNameToIDMap.emplace(newName, tableID);
    tableSchema->updateTableName(newName);
}

void CatalogContent::saveToFile(const std::string& directory, DBFileType dbFileType) {
    auto catalogPath = StorageUtils::getCatalogFilePath(directory, dbFileType);
    auto fileInfo = FileUtils::openFile(catalogPath, O_WRONLY | O_CREAT);
    uint64_t offset = 0;
    writeMagicBytes(fileInfo.get(), offset);
    SerDeser::serializeValue(StorageVersionInfo::getStorageVersion(), fileInfo.get(), offset);
    SerDeser::serializeValue(tableSchemas.size(), fileInfo.get(), offset);
    for (auto& [tableID, tableSchema] : tableSchemas) {
        SerDeser::serializeValue(tableID, fileInfo.get(), offset);
        tableSchema->serialize(fileInfo.get(), offset);
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
    uint64_t numTables;
    SerDeser::deserializeValue(numTables, fileInfo.get(), offset);
    table_id_t tableID;
    for (auto i = 0u; i < numTables; i++) {
        SerDeser::deserializeValue(tableID, fileInfo.get(), offset);
        tableSchemas[tableID] = TableSchema::deserialize(fileInfo.get(), offset);
    }
    for (auto& [tableID_, tableSchema] : tableSchemas) {
        tableNameToIDMap[tableSchema->tableName] = tableID_;
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

std::unique_ptr<CatalogContent> CatalogContent::copy() const {
    std::unordered_map<table_id_t, std::unique_ptr<TableSchema>> tableSchemasToCopy;
    for (auto& [tableID, tableSchema] : tableSchemas) {
        tableSchemasToCopy.emplace(tableID, tableSchema->copy());
    }
    std::unordered_map<std::string, std::unique_ptr<function::ScalarMacroFunction>> macrosToCopy;
    for (auto& macro : macros) {
        macrosToCopy.emplace(macro.first, macro.second->copy());
    }
    return std::make_unique<CatalogContent>(
        std::move(tableSchemasToCopy), tableNameToIDMap, nextTableID, std::move(macrosToCopy));
}

void CatalogContent::validateStorageVersion(storage_version_t savedStorageVersion) {
    auto storageVersion = StorageVersionInfo::getStorageVersion();
    if (savedStorageVersion != storageVersion) {
        throw RuntimeException(StringUtils::string_format(
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
        throw RuntimeException(
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

std::vector<table_id_t> CatalogContent::getTableIDsByType(TableType tableType) const {
    std::vector<table_id_t> tableIDs;
    for (auto& tableSchema : tableSchemas) {
        if (tableSchema.second->getTableType() == tableType) {
            tableIDs.push_back(tableSchema.first);
        }
    }
    return tableIDs;
}

} // namespace catalog
} // namespace kuzu
