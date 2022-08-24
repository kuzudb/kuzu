#include "include/catalog.h"

#include "spdlog/spdlog.h"

#include "src/storage/include/storage_utils.h"

using namespace std;
using namespace graphflow::catalog;

namespace graphflow {
namespace common {

/**
 * Specialized serialize and deserialize functions used in Catalog.
 * */

template<>
uint64_t SerDeser::serializeValue<string>(
    const string& value, FileInfo* fileInfo, uint64_t offset) {
    uint64_t valueLength = value.length();
    FileUtils::writeToFile(fileInfo, (uint8_t*)&valueLength, sizeof(uint64_t), offset);
    FileUtils::writeToFile(
        fileInfo, (uint8_t*)value.data(), valueLength, offset + sizeof(uint64_t));
    return offset + sizeof(uint64_t) + valueLength;
}

template<>
uint64_t SerDeser::deserializeValue<string>(string& value, FileInfo* fileInfo, uint64_t offset) {
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
    if (value.childType) {
        assert(value.typeID == LIST);
        return SerDeser::serializeValue<DataType>(*value.childType, fileInfo, offset);
    }
    return offset;
}

template<>
uint64_t SerDeser::deserializeValue<DataType>(
    DataType& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<DataTypeID>(value.typeID, fileInfo, offset);
    if (value.typeID == LIST) {
        auto childDataType = make_unique<DataType>();
        offset = SerDeser::deserializeValue<DataType>(*childDataType, fileInfo, offset);
        value.childType = move(childDataType);
        return offset;
    }
    return offset;
}

template<>
uint64_t SerDeser::serializeValue<Property>(
    const Property& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<string>(value.name, fileInfo, offset);
    offset = SerDeser::serializeValue<DataType>(value.dataType, fileInfo, offset);
    offset = SerDeser::serializeValue<uint32_t>(value.propertyID, fileInfo, offset);
    offset = SerDeser::serializeValue<table_id_t>(value.tableID, fileInfo, offset);
    return SerDeser::serializeValue<bool>(value.isIDProperty(), fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue<Property>(
    Property& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<string>(value.name, fileInfo, offset);
    offset = SerDeser::deserializeValue<DataType>(value.dataType, fileInfo, offset);
    offset = SerDeser::deserializeValue<uint32_t>(value.propertyID, fileInfo, offset);
    offset = SerDeser::deserializeValue<table_id_t>(value.tableID, fileInfo, offset);
    bool isIDProperty = value.isIDProperty();
    return SerDeser::deserializeValue<bool>(isIDProperty, fileInfo, offset);
}

template<>
uint64_t SerDeser::serializeValue(
    const unordered_map<table_id_t, uint64_t>& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<uint64_t>(value.size(), fileInfo, offset);
    for (auto& entry : value) {
        offset = SerDeser::serializeValue<table_id_t>(entry.first, fileInfo, offset);
        offset = SerDeser::serializeValue<uint64_t>(entry.second, fileInfo, offset);
    }
    return offset;
}

template<>
uint64_t SerDeser::deserializeValue(
    unordered_map<table_id_t, uint64_t>& value, FileInfo* fileInfo, uint64_t offset) {
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
uint64_t SerDeser::serializeVector<vector<uint64_t>>(
    const vector<vector<uint64_t>>& values, FileInfo* fileInfo, uint64_t offset) {
    uint64_t vectorSize = values.size();
    offset = SerDeser::serializeValue<uint64_t>(vectorSize, fileInfo, offset);
    for (auto& value : values) {
        offset = serializeVector<uint64_t>(value, fileInfo, offset);
    }
    return offset;
}

template<>
uint64_t SerDeser::deserializeVector<vector<uint64_t>>(
    vector<vector<uint64_t>>& values, FileInfo* fileInfo, uint64_t offset) {
    uint64_t vectorSize;
    offset = deserializeValue<uint64_t>(vectorSize, fileInfo, offset);
    values.resize(vectorSize);
    for (auto& value : values) {
        offset = SerDeser::deserializeVector<uint64_t>(value, fileInfo, offset);
    }
    return offset;
}

template<>
uint64_t SerDeser::serializeValue<NodeTableSchema>(
    const NodeTableSchema& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<string>(value.tableName, fileInfo, offset);
    offset = SerDeser::serializeValue<table_id_t>(value.tableID, fileInfo, offset);
    offset = SerDeser::serializeValue<uint64_t>(value.primaryPropertyId, fileInfo, offset);
    offset = SerDeser::serializeVector<Property>(value.structuredProperties, fileInfo, offset);
    offset = SerDeser::serializeVector<Property>(value.unstructuredProperties, fileInfo, offset);
    offset = SerDeser::serializeUnorderedSet<table_id_t>(value.fwdRelTableIDSet, fileInfo, offset);
    return SerDeser::serializeUnorderedSet<table_id_t>(value.bwdRelTableIDSet, fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue<NodeTableSchema>(
    NodeTableSchema& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<string>(value.tableName, fileInfo, offset);
    offset = SerDeser::deserializeValue<table_id_t>(value.tableID, fileInfo, offset);
    offset = SerDeser::deserializeValue<uint64_t>(value.primaryPropertyId, fileInfo, offset);
    offset = SerDeser::deserializeVector<Property>(value.structuredProperties, fileInfo, offset);
    offset = SerDeser::deserializeVector<Property>(value.unstructuredProperties, fileInfo, offset);
    offset =
        SerDeser::deserializeUnorderedSet<table_id_t>(value.fwdRelTableIDSet, fileInfo, offset);
    return SerDeser::deserializeUnorderedSet<table_id_t>(value.bwdRelTableIDSet, fileInfo, offset);
}

template<>
uint64_t SerDeser::serializeValue<RelTableSchema>(
    const RelTableSchema& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<string>(value.tableName, fileInfo, offset);
    offset = SerDeser::serializeValue<table_id_t>(value.tableID, fileInfo, offset);
    offset = SerDeser::serializeValue<RelMultiplicity>(value.relMultiplicity, fileInfo, offset);
    offset = SerDeser::serializeVector<Property>(value.properties, fileInfo, offset);
    offset = SerDeser::serializeUnorderedSet<table_id_t>(
        value.srcDstTableIDs.srcTableIDs, fileInfo, offset);
    offset = SerDeser::serializeUnorderedSet<table_id_t>(
        value.srcDstTableIDs.dstTableIDs, fileInfo, offset);
    return SerDeser::serializeVector<unordered_map<table_id_t, uint64_t>>(
        value.numRelsPerDirectionBoundTableID, fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue<RelTableSchema>(
    RelTableSchema& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<string>(value.tableName, fileInfo, offset);
    offset = SerDeser::deserializeValue<table_id_t>(value.tableID, fileInfo, offset);
    offset = SerDeser::deserializeValue<RelMultiplicity>(value.relMultiplicity, fileInfo, offset);
    offset = SerDeser::deserializeVector<Property>(value.properties, fileInfo, offset);
    offset = SerDeser::deserializeUnorderedSet<table_id_t>(
        value.srcDstTableIDs.srcTableIDs, fileInfo, offset);
    offset = SerDeser::deserializeUnorderedSet<table_id_t>(
        value.srcDstTableIDs.dstTableIDs, fileInfo, offset);
    return SerDeser::deserializeVector<unordered_map<table_id_t, uint64_t>>(
        value.numRelsPerDirectionBoundTableID, fileInfo, offset);
}

} // namespace common
} // namespace graphflow

namespace graphflow {
namespace catalog {

CatalogContent::CatalogContent() {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
}

CatalogContent::CatalogContent(const string& directory) {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
    logger->info("Initializing catalog.");
    readFromFile(directory, DBFileType::ORIGINAL);
    logger->info("Initializing catalog done.");
}

CatalogContent::CatalogContent(const CatalogContent& other) {
    for (auto& nodeTableSchema : other.nodeTableSchemas) {
        nodeTableSchemas.push_back(make_unique<NodeTableSchema>(*nodeTableSchema));
    }
    for (auto& relTableSchema : other.relTableSchemas) {
        relTableSchemas.push_back(make_unique<RelTableSchema>(*relTableSchema));
    }
    nodeTableNameToIDMap = other.nodeTableNameToIDMap;
    relTableNameToIDMap = other.relTableNameToIDMap;
}

table_id_t CatalogContent::addNodeTableSchema(string tableName, uint32_t primaryKeyIdx,
    vector<PropertyNameDataType> structuredPropertyDefinitions) {
    table_id_t tableID = getNumNodeTables();
    vector<Property> structuredProperties;
    for (auto i = 0u; i < structuredPropertyDefinitions.size(); ++i) {
        auto& propertyDefinition = structuredPropertyDefinitions[i];
        structuredProperties.push_back(
            Property::constructStructuredNodeProperty(propertyDefinition, i, tableID));
    }
    auto nodeTableSchema = make_unique<NodeTableSchema>(
        move(tableName), tableID, primaryKeyIdx, move(structuredProperties));
    nodeTableNameToIDMap[nodeTableSchema->tableName] = tableID;
    nodeTableSchemas.push_back(move(nodeTableSchema));
    return tableID;
}

table_id_t CatalogContent::addRelTableSchema(string tableName, RelMultiplicity relMultiplicity,
    vector<PropertyNameDataType> structuredPropertyDefinitions, SrcDstTableIDs srcDstTableIDs) {
    table_id_t tableID = relTableSchemas.size();
    for (auto& srcTableID : srcDstTableIDs.srcTableIDs) {
        nodeTableSchemas[srcTableID]->addFwdRelTableID(tableID);
    }
    for (auto& dstTableID : srcDstTableIDs.dstTableIDs) {
        nodeTableSchemas[dstTableID]->addBwdRelTableID(tableID);
    }
    vector<Property> structuredProperties;
    auto propertyId = 0;
    for (auto& propertyDefinition : structuredPropertyDefinitions) {
        structuredProperties.push_back(
            Property::constructRelProperty(propertyDefinition, propertyId++, tableID));
    }
    auto propertyNameDataType = PropertyNameDataType(INTERNAL_ID_SUFFIX, INT64);
    structuredProperties.push_back(
        Property::constructRelProperty(propertyNameDataType, propertyId++, tableID));
    auto relTableSchema = make_unique<RelTableSchema>(move(tableName), tableID, relMultiplicity,
        move(structuredProperties), move(srcDstTableIDs));
    relTableNameToIDMap[relTableSchema->tableName] = tableID;
    relTableSchemas.push_back(move(relTableSchema));
    return tableID;
}

bool CatalogContent::containNodeProperty(table_id_t tableID, const string& propertyName) const {
    for (auto& property : nodeTableSchemas[tableID]->structuredProperties) {
        if (propertyName == property.name) {
            return true;
        }
    }
    return nodeTableSchemas[tableID]->unstrPropertiesNameToIdMap.contains(propertyName);
}

bool CatalogContent::containRelProperty(table_id_t tableID, const string& propertyName) const {
    for (auto& property : relTableSchemas[tableID]->properties) {
        if (propertyName == property.name) {
            return true;
        }
    }
    return false;
}

const Property& CatalogContent::getNodeProperty(
    table_id_t tableID, const string& propertyName) const {
    for (auto& property : nodeTableSchemas[tableID]->structuredProperties) {
        if (propertyName == property.name) {
            return property;
        }
    }
    auto unstrPropertyIdx = getUnstrPropertiesNameToIdMap(tableID).at(propertyName);
    return getUnstructuredNodeProperties(tableID)[unstrPropertyIdx];
}

const Property& CatalogContent::getRelProperty(
    table_id_t tableID, const string& propertyName) const {
    for (auto& property : relTableSchemas[tableID]->properties) {
        if (propertyName == property.name) {
            return property;
        }
    }
    assert(false);
}

const Property& CatalogContent::getNodePrimaryKeyProperty(table_id_t tableID) const {
    auto primaryKeyId = nodeTableSchemas[tableID]->primaryPropertyId;
    return nodeTableSchemas[tableID]->structuredProperties[primaryKeyId];
}

vector<Property> CatalogContent::getAllNodeProperties(table_id_t tableID) const {
    return nodeTableSchemas[tableID]->getAllNodeProperties();
}

const unordered_set<table_id_t>& CatalogContent::getRelTableIDsForNodeTableDirection(
    table_id_t tableID, RelDirection direction) const {
    if (tableID >= nodeTableSchemas.size()) {
        throw CatalogException("Node table " + to_string(tableID) + " is out of bounds.");
    }
    if (FWD == direction) {
        return nodeTableSchemas[tableID]->fwdRelTableIDSet;
    }
    return nodeTableSchemas[tableID]->bwdRelTableIDSet;
}

const unordered_set<table_id_t>& CatalogContent::getNodeTableIDsForRelTableDirection(
    table_id_t tableID, RelDirection direction) const {
    if (tableID >= relTableSchemas.size()) {
        throw CatalogException("Rel table " + to_string(tableID) + " is out of bounds.");
    }
    if (FWD == direction) {
        return relTableSchemas[tableID]->srcDstTableIDs.srcTableIDs;
    }
    return relTableSchemas[tableID]->srcDstTableIDs.dstTableIDs;
}

bool CatalogContent::isSingleMultiplicityInDirection(
    table_id_t tableID, RelDirection direction) const {
    auto relMultiplicity = relTableSchemas[tableID]->relMultiplicity;
    if (FWD == direction) {
        return ONE_ONE == relMultiplicity || MANY_ONE == relMultiplicity;
    } else {
        return ONE_ONE == relMultiplicity || ONE_MANY == relMultiplicity;
    }
}

uint64_t CatalogContent::getNumRelsForDirectionBoundTableID(
    table_id_t tableID, RelDirection relDirection, table_id_t boundNodeTableID) const {
    if (tableID >= relTableSchemas.size()) {
        throw CatalogException("Rel table " + to_string(tableID) + " is out of bounds.");
    }
    return relTableSchemas[tableID]
        ->numRelsPerDirectionBoundTableID[FWD == relDirection ? 0 : 1]
        .at(boundNodeTableID);
}

void CatalogContent::saveToFile(const string& directory, DBFileType dbFileType) {
    auto catalogPath = StorageUtils::getCatalogFilePath(directory, dbFileType);
    auto fileInfo = FileUtils::openFile(catalogPath, O_WRONLY | O_CREAT);
    uint64_t offset = 0;
    offset = SerDeser::serializeValue<uint64_t>(nodeTableSchemas.size(), fileInfo.get(), offset);
    offset = SerDeser::serializeValue<uint64_t>(relTableSchemas.size(), fileInfo.get(), offset);
    for (auto& nodeTableSchema : nodeTableSchemas) {
        offset =
            SerDeser::serializeValue<NodeTableSchema>(*nodeTableSchema, fileInfo.get(), offset);
    }
    for (auto& relTableSchema : relTableSchemas) {
        offset = SerDeser::serializeValue<RelTableSchema>(*relTableSchema, fileInfo.get(), offset);
    }
    FileUtils::closeFile(fileInfo->fd);
}

void CatalogContent::readFromFile(const string& directory, DBFileType dbFileType) {
    auto catalogPath = StorageUtils::getCatalogFilePath(directory, dbFileType);
    logger->debug("Reading from {}.", catalogPath);
    auto fileInfo = FileUtils::openFile(catalogPath, O_RDONLY);
    uint64_t offset = 0;
    uint64_t numNodeTables, numRelTables;
    offset = SerDeser::deserializeValue<uint64_t>(numNodeTables, fileInfo.get(), offset);
    offset = SerDeser::deserializeValue<uint64_t>(numRelTables, fileInfo.get(), offset);
    nodeTableSchemas.resize(numNodeTables);
    relTableSchemas.resize(numRelTables);
    for (auto tableID = 0u; tableID < numNodeTables; tableID++) {
        nodeTableSchemas[tableID] = make_unique<NodeTableSchema>();
        offset = SerDeser::deserializeValue<NodeTableSchema>(
            *nodeTableSchemas[tableID], fileInfo.get(), offset);
    }
    for (auto tableID = 0u; tableID < numRelTables; tableID++) {
        relTableSchemas[tableID] = make_unique<RelTableSchema>();
        offset = SerDeser::deserializeValue<RelTableSchema>(
            *relTableSchemas[tableID], fileInfo.get(), offset);
    }
    // construct the tableNameToIdMap and table's unstrPropertiesNameToIdMap
    for (auto& nodeTableSchema : nodeTableSchemas) {
        nodeTableNameToIDMap[nodeTableSchema->tableName] = nodeTableSchema->tableID;
        for (auto i = 0u; i < nodeTableSchema->unstructuredProperties.size(); i++) {
            auto& property = nodeTableSchema->unstructuredProperties[i];
            if (property.dataType.typeID == UNSTRUCTURED) {
                nodeTableSchema->unstrPropertiesNameToIdMap[property.name] = property.propertyID;
            }
        }
    }
    for (auto& relTableSchema : relTableSchemas) {
        relTableNameToIDMap[relTableSchema->tableName] = relTableSchema->tableID;
    }
    FileUtils::closeFile(fileInfo->fd);
}

uint64_t CatalogContent::getNextRelID() const {
    auto nextRelID = 0ull;
    for (auto& relTableSchema : relTableSchemas) {
        nextRelID += relTableSchema->getNumRels();
    }
    return nextRelID;
}

Catalog::Catalog() {
    catalogContentForReadOnlyTrx = make_unique<CatalogContent>();
    builtInVectorOperations = make_unique<BuiltInVectorOperations>();
    builtInAggregateFunctions = make_unique<BuiltInAggregateFunctions>();
}

Catalog::Catalog(WAL* wal) : wal{wal} {
    catalogContentForReadOnlyTrx = make_unique<CatalogContent>(wal->getDirectory());
    builtInVectorOperations = make_unique<BuiltInVectorOperations>();
    builtInAggregateFunctions = make_unique<BuiltInAggregateFunctions>();
}

void Catalog::checkpointInMemoryIfNecessary() {
    if (!hasUpdates()) {
        return;
    }
    catalogContentForReadOnlyTrx = move(catalogContentForWriteTrx);
}

ExpressionType Catalog::getFunctionType(const string& name) const {
    if (builtInVectorOperations->containsFunction(name)) {
        return FUNCTION;
    } else if (builtInAggregateFunctions->containsFunction(name)) {
        return AGGREGATE_FUNCTION;
    } else {
        throw CatalogException(name + " function does not exist.");
    }
}

table_id_t Catalog::addNodeTableSchema(string tableName, uint32_t primaryKeyIdx,
    vector<PropertyNameDataType> structuredPropertyDefinitions) {
    initCatalogContentForWriteTrxIfNecessary();
    auto tableID = catalogContentForWriteTrx->addNodeTableSchema(
        move(tableName), primaryKeyIdx, move(structuredPropertyDefinitions));
    wal->logNodeTableRecord(tableID);
    return tableID;
}

table_id_t Catalog::addRelTableSchema(string tableName, RelMultiplicity relMultiplicity,
    vector<PropertyNameDataType> structuredPropertyDefinitions, SrcDstTableIDs srcDstTableIDs) {
    initCatalogContentForWriteTrxIfNecessary();
    auto tableID = catalogContentForWriteTrx->addRelTableSchema(move(tableName), relMultiplicity,
        move(structuredPropertyDefinitions), move(srcDstTableIDs));
    wal->logRelTableRecord(tableID);
    return tableID;
}

void Catalog::setNumRelsPerDirectionBoundTableIDOfRelTableSchema(
    vector<unique_ptr<atomic_uint64_vec_t>>& directionNumRelsPerTable, table_id_t tableID) {
    initCatalogContentForWriteTrxIfNecessary();
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto boundNodeTableID :
            getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(tableID, relDirection)) {
            catalogContentForWriteTrx->getRelTableSchema(tableID)
                ->numRelsPerDirectionBoundTableID[relDirection][boundNodeTableID] =
                directionNumRelsPerTable[relDirection]->operator[](boundNodeTableID).load();
        }
    }
}

} // namespace catalog
} // namespace graphflow
