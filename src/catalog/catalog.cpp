#include "catalog/catalog.h"

#include "common/ser_deser.h"
#include "common/string_utils.h"
#include "spdlog/spdlog.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace common {

/**
 * Specialized serialize and deserialize functions used in Catalog.
 * */

template<>
void SerDeser::serializeValue<Property>(
    const Property& value, FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::serializeValue<std::string>(value.name, fileInfo, offset);
    SerDeser::serializeValue<LogicalType>(value.dataType, fileInfo, offset);
    SerDeser::serializeValue<property_id_t>(value.propertyID, fileInfo, offset);
    SerDeser::serializeValue<table_id_t>(value.tableID, fileInfo, offset);
}

template<>
void SerDeser::deserializeValue<Property>(Property& value, FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::deserializeValue<std::string>(value.name, fileInfo, offset);
    SerDeser::deserializeValue<LogicalType>(value.dataType, fileInfo, offset);
    SerDeser::deserializeValue<property_id_t>(value.propertyID, fileInfo, offset);
    SerDeser::deserializeValue<table_id_t>(value.tableID, fileInfo, offset);
}

template<>
void SerDeser::serializeValue(
    const std::unordered_map<table_id_t, uint64_t>& value, FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::serializeValue<uint64_t>(value.size(), fileInfo, offset);
    for (auto& entry : value) {
        SerDeser::serializeValue<table_id_t>(entry.first, fileInfo, offset);
        SerDeser::serializeValue<uint64_t>(entry.second, fileInfo, offset);
    }
}

template<>
void SerDeser::deserializeValue(
    std::unordered_map<table_id_t, uint64_t>& value, FileInfo* fileInfo, uint64_t& offset) {
    uint64_t numEntries = 0;
    deserializeValue(numEntries, fileInfo, offset);
    for (auto i = 0u; i < numEntries; i++) {
        table_id_t tableID;
        uint64_t num;
        deserializeValue<table_id_t>(tableID, fileInfo, offset);
        deserializeValue<uint64_t>(num, fileInfo, offset);
        value.emplace(tableID, num);
    }
}

template<>
void SerDeser::serializeVector<std::vector<uint64_t>>(
    const std::vector<std::vector<uint64_t>>& values, FileInfo* fileInfo, uint64_t& offset) {
    uint64_t vectorSize = values.size();
    SerDeser::serializeValue<uint64_t>(vectorSize, fileInfo, offset);
    for (auto& value : values) {
        serializeVector<uint64_t>(value, fileInfo, offset);
    }
}

template<>
void SerDeser::serializeValue<TableSchema>(
    const TableSchema& value, FileInfo* fileInfo, uint64_t& offset) {
    serializeValue<std::string>(value.tableName, fileInfo, offset);
    serializeValue<table_id_t>(value.tableID, fileInfo, offset);
    serializeVector<Property>(value.properties, fileInfo, offset);
    serializeValue<property_id_t>(value.nextPropertyID, fileInfo, offset);
}

template<>
void SerDeser::deserializeValue<TableSchema>(
    TableSchema& value, FileInfo* fileInfo, uint64_t& offset) {
    deserializeValue<std::string>(value.tableName, fileInfo, offset);
    deserializeValue<table_id_t>(value.tableID, fileInfo, offset);
    deserializeVector<Property>(value.properties, fileInfo, offset);
    deserializeValue<property_id_t>(value.nextPropertyID, fileInfo, offset);
}

template<>
void SerDeser::serializeValue<NodeTableSchema>(
    const NodeTableSchema& value, FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::serializeValue<TableSchema>((const TableSchema&)value, fileInfo, offset);
    SerDeser::serializeValue<property_id_t>(value.primaryKeyPropertyID, fileInfo, offset);
    SerDeser::serializeUnorderedSet<table_id_t>(value.fwdRelTableIDSet, fileInfo, offset);
    SerDeser::serializeUnorderedSet<table_id_t>(value.bwdRelTableIDSet, fileInfo, offset);
}

template<>
void SerDeser::deserializeValue<NodeTableSchema>(
    NodeTableSchema& value, FileInfo* fileInfo, uint64_t& offset) {
    deserializeValue<TableSchema>((TableSchema&)value, fileInfo, offset);
    deserializeValue<property_id_t>(value.primaryKeyPropertyID, fileInfo, offset);
    deserializeUnorderedSet<table_id_t>(value.fwdRelTableIDSet, fileInfo, offset);
    deserializeUnorderedSet<table_id_t>(value.bwdRelTableIDSet, fileInfo, offset);
}

template<>
void SerDeser::serializeValue<RelTableSchema>(
    const RelTableSchema& value, FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::serializeValue<TableSchema>((const TableSchema&)value, fileInfo, offset);
    SerDeser::serializeValue<RelMultiplicity>(value.relMultiplicity, fileInfo, offset);
    SerDeser::serializeValue<table_id_t>(value.srcTableID, fileInfo, offset);
    SerDeser::serializeValue<table_id_t>(value.dstTableID, fileInfo, offset);
    SerDeser::serializeValue<LogicalType>(value.srcPKDataType, fileInfo, offset);
    SerDeser::serializeValue<LogicalType>(value.dstPKDataType, fileInfo, offset);
}

template<>
void SerDeser::deserializeValue<RelTableSchema>(
    RelTableSchema& value, FileInfo* fileInfo, uint64_t& offset) {
    deserializeValue<TableSchema>((TableSchema&)value, fileInfo, offset);
    deserializeValue<RelMultiplicity>(value.relMultiplicity, fileInfo, offset);
    deserializeValue<table_id_t>(value.srcTableID, fileInfo, offset);
    deserializeValue<table_id_t>(value.dstTableID, fileInfo, offset);
    deserializeValue<LogicalType>(value.srcPKDataType, fileInfo, offset);
    deserializeValue<LogicalType>(value.dstPKDataType, fileInfo, offset);
}

} // namespace common

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
    auto relTableSchema =
        std::make_unique<RelTableSchema>(std::move(tableName), tableID, relMultiplicity,
            std::move(properties), srcTableID, dstTableID, srcPKDataType, dstPKDataType);
    relTableNameToIDMap[relTableSchema->tableName] = tableID;
    relTableSchemas[tableID] = std::move(relTableSchema);
    return tableID;
}

const Property& CatalogContent::getNodeProperty(
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
    if (tableSchema->isNodeTable) {
        nodeTableNameToIDMap.erase(tableSchema->tableName);
        nodeTableSchemas.erase(tableID);
    } else {
        relTableNameToIDMap.erase(tableSchema->tableName);
        relTableSchemas.erase(tableID);
    }
}

void CatalogContent::renameTable(table_id_t tableID, const std::string& newName) {
    auto tableSchema = getTableSchema(tableID);
    auto& tableNameToIDMap = tableSchema->isNodeTable ? nodeTableNameToIDMap : relTableNameToIDMap;
    tableNameToIDMap.erase(tableSchema->tableName);
    tableNameToIDMap.emplace(newName, tableID);
    tableSchema->tableName = newName;
}

void CatalogContent::saveToFile(const std::string& directory, DBFileType dbFileType) {
    auto catalogPath = StorageUtils::getCatalogFilePath(directory, dbFileType);
    auto fileInfo = FileUtils::openFile(catalogPath, O_WRONLY | O_CREAT);
    uint64_t offset = 0;
    writeMagicBytes(fileInfo.get(), offset);
    SerDeser::serializeValue<uint64_t>(
        StorageVersionInfo::getStorageVersion(), fileInfo.get(), offset);
    SerDeser::serializeValue<uint64_t>(nodeTableSchemas.size(), fileInfo.get(), offset);
    SerDeser::serializeValue<uint64_t>(relTableSchemas.size(), fileInfo.get(), offset);
    for (auto& nodeTableSchema : nodeTableSchemas) {
        SerDeser::serializeValue<uint64_t>(nodeTableSchema.first, fileInfo.get(), offset);
        SerDeser::serializeValue<NodeTableSchema>(*nodeTableSchema.second, fileInfo.get(), offset);
    }
    for (auto& relTableSchema : relTableSchemas) {
        SerDeser::serializeValue<uint64_t>(relTableSchema.first, fileInfo.get(), offset);
        SerDeser::serializeValue<RelTableSchema>(*relTableSchema.second, fileInfo.get(), offset);
    }
    SerDeser::serializeValue(nextTableID, fileInfo.get(), offset);
    SerDeser::serializeUnorderedMap(macros, fileInfo.get(), offset);
}

void CatalogContent::readFromFile(const std::string& directory, DBFileType dbFileType) {
    auto catalogPath = StorageUtils::getCatalogFilePath(directory, dbFileType);
    auto fileInfo = FileUtils::openFile(catalogPath, O_RDONLY);
    uint64_t offset = 0;
    validateMagicBytes(fileInfo.get(), offset);
    storage::storage_version_t savedStorageVersion;
    SerDeser::deserializeValue<uint64_t>(savedStorageVersion, fileInfo.get(), offset);
    validateStorageVersion(savedStorageVersion);
    uint64_t numNodeTables, numRelTables;
    SerDeser::deserializeValue<uint64_t>(numNodeTables, fileInfo.get(), offset);
    SerDeser::deserializeValue<uint64_t>(numRelTables, fileInfo.get(), offset);
    table_id_t tableID;
    for (auto i = 0u; i < numNodeTables; i++) {
        SerDeser::deserializeValue<uint64_t>(tableID, fileInfo.get(), offset);
        nodeTableSchemas[tableID] = std::make_unique<NodeTableSchema>();
        SerDeser::deserializeValue<NodeTableSchema>(
            *nodeTableSchemas[tableID], fileInfo.get(), offset);
    }
    for (auto i = 0u; i < numRelTables; i++) {
        SerDeser::deserializeValue<uint64_t>(tableID, fileInfo.get(), offset);
        relTableSchemas[tableID] = std::make_unique<RelTableSchema>();
        SerDeser::deserializeValue<RelTableSchema>(
            *relTableSchemas[tableID], fileInfo.get(), offset);
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

void CatalogContent::validateStorageVersion(storage_version_t savedStorageVersion) const {
    auto storageVersion = StorageVersionInfo::getStorageVersion();
    if (savedStorageVersion != storageVersion) {
        throw common::RuntimeException(StringUtils::string_format(
            "Trying to read a database file with a different version. "
            "Database file version: {}, Current build storage version: {}",
            savedStorageVersion, storageVersion));
    }
}

void CatalogContent::validateMagicBytes(FileInfo* fileInfo, offset_t& offset) const {
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

void CatalogContent::writeMagicBytes(FileInfo* fileInfo, offset_t& offset) const {
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

Catalog::Catalog() : wal{nullptr} {
    catalogContentForReadOnlyTrx = std::make_unique<CatalogContent>();
}

Catalog::Catalog(WAL* wal) : wal{wal} {
    catalogContentForReadOnlyTrx = std::make_unique<CatalogContent>(wal->getDirectory());
}

void Catalog::prepareCommitOrRollback(TransactionAction action) {
    if (hasUpdates()) {
        wal->logCatalogRecord();
        if (action == TransactionAction::COMMIT) {
            catalogContentForWriteTrx->saveToFile(
                wal->getDirectory(), common::DBFileType::WAL_VERSION);
        }
    }
}

void Catalog::checkpointInMemory() {
    if (hasUpdates()) {
        catalogContentForReadOnlyTrx = std::move(catalogContentForWriteTrx);
    }
}

ExpressionType Catalog::getFunctionType(const std::string& name) const {
    return catalogContentForReadOnlyTrx->getFunctionType(name);
}

table_id_t Catalog::addNodeTableSchema(
    std::string tableName, property_id_t primaryKeyId, std::vector<Property> propertyDefinitions) {
    initCatalogContentForWriteTrxIfNecessary();
    auto tableID = catalogContentForWriteTrx->addNodeTableSchema(
        std::move(tableName), primaryKeyId, std::move(propertyDefinitions));
    wal->logNodeTableRecord(tableID);
    return tableID;
}

table_id_t Catalog::addRelTableSchema(std::string tableName, RelMultiplicity relMultiplicity,
    const std::vector<Property>& propertyDefinitions, table_id_t srcTableID, table_id_t dstTableID,
    LogicalType srcPKDataType, LogicalType dstPKDataType) {
    initCatalogContentForWriteTrxIfNecessary();
    auto tableID = catalogContentForWriteTrx->addRelTableSchema(std::move(tableName),
        relMultiplicity, propertyDefinitions, srcTableID, dstTableID, srcPKDataType, dstPKDataType);
    wal->logRelTableRecord(tableID);
    return tableID;
}

void Catalog::dropTableSchema(table_id_t tableID) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->dropTableSchema(tableID);
    wal->logDropTableRecord(tableID);
}

void Catalog::renameTable(table_id_t tableID, std::string newName) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->renameTable(tableID, std::move(newName));
}

void Catalog::addProperty(
    table_id_t tableID, const std::string& propertyName, LogicalType dataType) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->addProperty(
        propertyName, std::move(dataType));
    wal->logAddPropertyRecord(
        tableID, catalogContentForWriteTrx->getTableSchema(tableID)->getPropertyID(propertyName));
}

void Catalog::dropProperty(table_id_t tableID, property_id_t propertyID) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->dropProperty(propertyID);
    wal->logDropPropertyRecord(tableID, propertyID);
}

void Catalog::renameProperty(
    table_id_t tableID, property_id_t propertyID, const std::string& newName) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->renameProperty(propertyID, newName);
}

std::unordered_set<RelTableSchema*> Catalog::getAllRelTableSchemasContainBoundTable(
    table_id_t boundTableID) const {
    std::unordered_set<RelTableSchema*> relTableSchemas;
    auto nodeTableSchema = getReadOnlyVersion()->getNodeTableSchema(boundTableID);
    for (auto& fwdRelTableID : nodeTableSchema->fwdRelTableIDSet) {
        relTableSchemas.insert(getReadOnlyVersion()->getRelTableSchema(fwdRelTableID));
    }
    for (auto& bwdRelTableID : nodeTableSchema->bwdRelTableIDSet) {
        relTableSchemas.insert(getReadOnlyVersion()->getRelTableSchema(bwdRelTableID));
    }
    return relTableSchemas;
}

void Catalog::addVectorFunction(
    std::string name, function::vector_function_definitions definitions) {
    catalogContentForReadOnlyTrx->addVectorFunction(std::move(name), std::move(definitions));
}

void Catalog::addScalarMacroFunction(
    std::string name, std::unique_ptr<function::ScalarMacroFunction> macro) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->addScalarMacroFunction(std::move(name), std::move(macro));
}

} // namespace catalog
} // namespace kuzu
