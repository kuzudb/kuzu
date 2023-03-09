#include "catalog/catalog.h"

#include "spdlog/spdlog.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::storage;

namespace kuzu {
namespace common {

/**
 * Specialized serialize and deserialize functions used in Catalog.
 * */

template<>
uint64_t SerDeser::serializeValue<std::string>(
    const std::string& value, FileInfo* fileInfo, uint64_t offset) {
    uint64_t valueLength = value.length();
    FileUtils::writeToFile(fileInfo, (uint8_t*)&valueLength, sizeof(uint64_t), offset);
    FileUtils::writeToFile(
        fileInfo, (uint8_t*)value.data(), valueLength, offset + sizeof(uint64_t));
    return offset + sizeof(uint64_t) + valueLength;
}

template<>
uint64_t SerDeser::deserializeValue<std::string>(
    std::string& value, FileInfo* fileInfo, uint64_t offset) {
    uint64_t valueLength = 0;
    offset = SerDeser::deserializeValue<uint64_t>(valueLength, fileInfo, offset);
    value.resize(valueLength);
    FileUtils::readFromFile(fileInfo, (uint8_t*)value.data(), valueLength, offset);
    return offset + valueLength;
}

template<>
uint64_t SerDeser::serializeValue<DataType>(
    const DataType& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<DataTypeID>(value.typeID, fileInfo, offset);
    offset = SerDeser::serializeValue<uint64_t>(value.fixedNumElementsInList, fileInfo, offset);
    if (value.typeID == VAR_LIST || value.typeID == FIXED_LIST) {
        offset = SerDeser::serializeValue<DataType>(*value.childType, fileInfo, offset);
    }
    return offset;
}

template<>
uint64_t SerDeser::deserializeValue<DataType>(
    DataType& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<DataTypeID>(value.typeID, fileInfo, offset);
    offset = SerDeser::deserializeValue<uint64_t>(value.fixedNumElementsInList, fileInfo, offset);
    if (value.typeID == VAR_LIST || value.typeID == FIXED_LIST) {
        auto childDataType = std::make_unique<DataType>();
        offset = SerDeser::deserializeValue<DataType>(*childDataType, fileInfo, offset);
        value.childType = std::move(childDataType);
    }
    return offset;
}

template<>
uint64_t SerDeser::serializeValue<Property>(
    const Property& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<std::string>(value.name, fileInfo, offset);
    offset = SerDeser::serializeValue<DataType>(value.dataType, fileInfo, offset);
    offset = SerDeser::serializeValue<property_id_t>(value.propertyID, fileInfo, offset);
    return SerDeser::serializeValue<table_id_t>(value.tableID, fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue<Property>(
    Property& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<std::string>(value.name, fileInfo, offset);
    offset = SerDeser::deserializeValue<DataType>(value.dataType, fileInfo, offset);
    offset = SerDeser::deserializeValue<property_id_t>(value.propertyID, fileInfo, offset);
    return SerDeser::deserializeValue<table_id_t>(value.tableID, fileInfo, offset);
}

template<>
uint64_t SerDeser::serializeValue(
    const std::unordered_map<table_id_t, uint64_t>& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<uint64_t>(value.size(), fileInfo, offset);
    for (auto& entry : value) {
        offset = SerDeser::serializeValue<table_id_t>(entry.first, fileInfo, offset);
        offset = SerDeser::serializeValue<uint64_t>(entry.second, fileInfo, offset);
    }
    return offset;
}

template<>
uint64_t SerDeser::deserializeValue(
    std::unordered_map<table_id_t, uint64_t>& value, FileInfo* fileInfo, uint64_t offset) {
    uint64_t numEntries = 0;
    offset = SerDeser::deserializeValue<uint64_t>(numEntries, fileInfo, offset);
    for (auto i = 0u; i < numEntries; i++) {
        table_id_t tableID;
        uint64_t num;
        offset = SerDeser::deserializeValue<table_id_t>(tableID, fileInfo, offset);
        offset = SerDeser::deserializeValue<uint64_t>(num, fileInfo, offset);
        value.emplace(tableID, num);
    }
    return offset;
}

template<>
uint64_t SerDeser::serializeVector<std::vector<uint64_t>>(
    const std::vector<std::vector<uint64_t>>& values, FileInfo* fileInfo, uint64_t offset) {
    uint64_t vectorSize = values.size();
    offset = SerDeser::serializeValue<uint64_t>(vectorSize, fileInfo, offset);
    for (auto& value : values) {
        offset = serializeVector<uint64_t>(value, fileInfo, offset);
    }
    return offset;
}

template<>
uint64_t SerDeser::deserializeVector<std::vector<uint64_t>>(
    std::vector<std::vector<uint64_t>>& values, FileInfo* fileInfo, uint64_t offset) {
    uint64_t vectorSize;
    offset = deserializeValue<uint64_t>(vectorSize, fileInfo, offset);
    values.resize(vectorSize);
    for (auto& value : values) {
        offset = SerDeser::deserializeVector<uint64_t>(value, fileInfo, offset);
    }
    return offset;
}

template<>
uint64_t SerDeser::serializeValue<TableSchema>(
    const TableSchema& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<std::string>(value.tableName, fileInfo, offset);
    offset = SerDeser::serializeValue<table_id_t>(value.tableID, fileInfo, offset);
    offset = SerDeser::serializeVector<Property>(value.properties, fileInfo, offset);
    return SerDeser::serializeValue<property_id_t>(value.nextPropertyID, fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue<TableSchema>(
    TableSchema& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<std::string>(value.tableName, fileInfo, offset);
    offset = SerDeser::deserializeValue<table_id_t>(value.tableID, fileInfo, offset);
    offset = SerDeser::deserializeVector<Property>(value.properties, fileInfo, offset);
    return SerDeser::deserializeValue<property_id_t>(value.nextPropertyID, fileInfo, offset);
}

template<>
uint64_t SerDeser::serializeValue<NodeTableSchema>(
    const NodeTableSchema& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<TableSchema>((const TableSchema&)value, fileInfo, offset);
    offset = SerDeser::serializeValue<property_id_t>(value.primaryKeyPropertyID, fileInfo, offset);
    offset = SerDeser::serializeUnorderedSet<table_id_t>(value.fwdRelTableIDSet, fileInfo, offset);
    return SerDeser::serializeUnorderedSet<table_id_t>(value.bwdRelTableIDSet, fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue<NodeTableSchema>(
    NodeTableSchema& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<TableSchema>((TableSchema&)value, fileInfo, offset);
    offset =
        SerDeser::deserializeValue<property_id_t>(value.primaryKeyPropertyID, fileInfo, offset);
    offset =
        SerDeser::deserializeUnorderedSet<table_id_t>(value.fwdRelTableIDSet, fileInfo, offset);
    return SerDeser::deserializeUnorderedSet<table_id_t>(value.bwdRelTableIDSet, fileInfo, offset);
}

template<>
uint64_t SerDeser::serializeValue<RelTableSchema>(
    const RelTableSchema& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<TableSchema>((const TableSchema&)value, fileInfo, offset);
    offset = SerDeser::serializeValue<RelMultiplicity>(value.relMultiplicity, fileInfo, offset);
    offset = SerDeser::serializeValue<table_id_t>(value.srcTableID, fileInfo, offset);
    return SerDeser::serializeValue<table_id_t>(value.dstTableID, fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue<RelTableSchema>(
    RelTableSchema& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<TableSchema>((TableSchema&)value, fileInfo, offset);
    offset = SerDeser::deserializeValue<RelMultiplicity>(value.relMultiplicity, fileInfo, offset);
    offset = SerDeser::deserializeValue<table_id_t>(value.srcTableID, fileInfo, offset);
    return SerDeser::deserializeValue<table_id_t>(value.dstTableID, fileInfo, offset);
}

} // namespace common
} // namespace kuzu

namespace kuzu {
namespace catalog {

CatalogContent::CatalogContent() : nextTableID{0} {
    logger = LoggerUtils::getLogger(LoggerConstants::LoggerEnum::CATALOG);
}

CatalogContent::CatalogContent(const std::string& directory) {
    logger = LoggerUtils::getLogger(LoggerConstants::LoggerEnum::CATALOG);
    logger->info("Initializing catalog.");
    readFromFile(directory, DBFileType::ORIGINAL);
    logger->info("Initializing catalog done.");
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
}

table_id_t CatalogContent::addNodeTableSchema(std::string tableName, property_id_t primaryKeyId,
    std::vector<PropertyNameDataType> propertyDefinitions) {
    table_id_t tableID = assignNextTableID();
    std::vector<Property> properties;
    for (auto i = 0u; i < propertyDefinitions.size(); ++i) {
        auto& propertyDefinition = propertyDefinitions[i];
        properties.push_back(Property::constructNodeProperty(propertyDefinition, i, tableID));
    }
    auto nodeTableSchema = std::make_unique<NodeTableSchema>(
        std::move(tableName), tableID, primaryKeyId, std::move(properties));
    nodeTableNameToIDMap[nodeTableSchema->tableName] = tableID;
    nodeTableSchemas[tableID] = std::move(nodeTableSchema);
    return tableID;
}

table_id_t CatalogContent::addRelTableSchema(std::string tableName, RelMultiplicity relMultiplicity,
    const std::vector<PropertyNameDataType>& propertyDefinitions, table_id_t srcTableID,
    table_id_t dstTableID) {
    table_id_t tableID = assignNextTableID();
    nodeTableSchemas[srcTableID]->addFwdRelTableID(tableID);
    nodeTableSchemas[dstTableID]->addBwdRelTableID(tableID);
    std::vector<Property> properties;
    auto propertyID = 0;
    auto propertyNameDataType = PropertyNameDataType(INTERNAL_ID_SUFFIX, INTERNAL_ID);
    properties.push_back(
        Property::constructRelProperty(propertyNameDataType, propertyID++, tableID));
    for (auto& propertyDefinition : propertyDefinitions) {
        properties.push_back(
            Property::constructRelProperty(propertyDefinition, propertyID++, tableID));
    }
    auto relTableSchema = std::make_unique<RelTableSchema>(std::move(tableName), tableID,
        relMultiplicity, std::move(properties), srcTableID, dstTableID);
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

std::vector<Property> CatalogContent::getAllNodeProperties(table_id_t tableID) const {
    return nodeTableSchemas.at(tableID)->getAllNodeProperties();
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

void CatalogContent::renameTable(table_id_t tableID, std::string newName) {
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
    offset = SerDeser::serializeValue<uint64_t>(nodeTableSchemas.size(), fileInfo.get(), offset);
    offset = SerDeser::serializeValue<uint64_t>(relTableSchemas.size(), fileInfo.get(), offset);
    for (auto& nodeTableSchema : nodeTableSchemas) {
        offset = SerDeser::serializeValue<uint64_t>(nodeTableSchema.first, fileInfo.get(), offset);
        offset = SerDeser::serializeValue<NodeTableSchema>(
            *nodeTableSchema.second, fileInfo.get(), offset);
    }
    for (auto& relTableSchema : relTableSchemas) {
        offset = SerDeser::serializeValue<uint64_t>(relTableSchema.first, fileInfo.get(), offset);
        offset = SerDeser::serializeValue<RelTableSchema>(
            *relTableSchema.second, fileInfo.get(), offset);
    }
    SerDeser::serializeValue<table_id_t>(nextTableID, fileInfo.get(), offset);
}

void CatalogContent::readFromFile(const std::string& directory, DBFileType dbFileType) {
    auto catalogPath = StorageUtils::getCatalogFilePath(directory, dbFileType);
    logger->debug("Reading from {}.", catalogPath);
    auto fileInfo = FileUtils::openFile(catalogPath, O_RDONLY);
    uint64_t offset = 0;
    uint64_t numNodeTables, numRelTables;
    offset = SerDeser::deserializeValue<uint64_t>(numNodeTables, fileInfo.get(), offset);
    offset = SerDeser::deserializeValue<uint64_t>(numRelTables, fileInfo.get(), offset);
    table_id_t tableID;
    for (auto i = 0u; i < numNodeTables; i++) {
        offset = SerDeser::deserializeValue<uint64_t>(tableID, fileInfo.get(), offset);
        nodeTableSchemas[tableID] = std::make_unique<NodeTableSchema>();
        offset = SerDeser::deserializeValue<NodeTableSchema>(
            *nodeTableSchemas[tableID], fileInfo.get(), offset);
    }
    for (auto i = 0u; i < numRelTables; i++) {
        offset = SerDeser::deserializeValue<uint64_t>(tableID, fileInfo.get(), offset);
        relTableSchemas[tableID] = std::make_unique<RelTableSchema>();
        offset = SerDeser::deserializeValue<RelTableSchema>(
            *relTableSchemas[tableID], fileInfo.get(), offset);
    }
    // Construct the tableNameToIdMap.
    for (auto& nodeTableSchema : nodeTableSchemas) {
        nodeTableNameToIDMap[nodeTableSchema.second->tableName] = nodeTableSchema.second->tableID;
    }
    for (auto& relTableSchema : relTableSchemas) {
        relTableNameToIDMap[relTableSchema.second->tableName] = relTableSchema.second->tableID;
    }
    SerDeser::deserializeValue<table_id_t>(nextTableID, fileInfo.get(), offset);
}

Catalog::Catalog() : wal{nullptr} {
    catalogContentForReadOnlyTrx = std::make_unique<CatalogContent>();
    builtInVectorOperations = std::make_unique<function::BuiltInVectorOperations>();
    builtInAggregateFunctions = std::make_unique<function::BuiltInAggregateFunctions>();
}

Catalog::Catalog(WAL* wal) : wal{wal} {
    catalogContentForReadOnlyTrx = std::make_unique<CatalogContent>(wal->getDirectory());
    builtInVectorOperations = std::make_unique<function::BuiltInVectorOperations>();
    builtInAggregateFunctions = std::make_unique<function::BuiltInAggregateFunctions>();
}

void Catalog::checkpointInMemoryIfNecessary() {
    if (!hasUpdates()) {
        return;
    }
    catalogContentForReadOnlyTrx = std::move(catalogContentForWriteTrx);
}

ExpressionType Catalog::getFunctionType(const std::string& name) const {
    if (builtInVectorOperations->containsFunction(name)) {
        return FUNCTION;
    } else if (builtInAggregateFunctions->containsFunction(name)) {
        return AGGREGATE_FUNCTION;
    } else {
        throw CatalogException(name + " function does not exist.");
    }
}

table_id_t Catalog::addNodeTableSchema(std::string tableName, property_id_t primaryKeyId,
    std::vector<PropertyNameDataType> propertyDefinitions) {
    initCatalogContentForWriteTrxIfNecessary();
    auto tableID = catalogContentForWriteTrx->addNodeTableSchema(
        std::move(tableName), primaryKeyId, std::move(propertyDefinitions));
    wal->logNodeTableRecord(tableID);
    return tableID;
}

table_id_t Catalog::addRelTableSchema(std::string tableName, RelMultiplicity relMultiplicity,
    const std::vector<PropertyNameDataType>& propertyDefinitions, table_id_t srcTableID,
    table_id_t dstTableID) {
    initCatalogContentForWriteTrxIfNecessary();
    auto tableID = catalogContentForWriteTrx->addRelTableSchema(
        std::move(tableName), relMultiplicity, propertyDefinitions, srcTableID, dstTableID);
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
    catalogContentForWriteTrx->renameTable(tableID, newName);
}

void Catalog::addProperty(table_id_t tableID, std::string propertyName, DataType dataType) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->addProperty(
        propertyName, std::move(dataType));
    wal->logAddPropertyRecord(tableID,
        catalogContentForWriteTrx->getTableSchema(tableID)->getPropertyID(std::move(propertyName)));
}

void Catalog::dropProperty(table_id_t tableID, property_id_t propertyID) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->dropProperty(propertyID);
    wal->logDropPropertyRecord(tableID, propertyID);
}

void Catalog::renameProperty(table_id_t tableID, property_id_t propertyID, std::string newName) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->renameProperty(propertyID, newName);
}

std::unordered_set<RelTableSchema*> Catalog::getAllRelTableSchemasContainBoundTable(
    table_id_t boundTableID) {
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

} // namespace catalog
} // namespace kuzu
