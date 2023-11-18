#include "catalog/catalog_content.h"

#include "catalog/node_table_schema.h"
#include "catalog/rdf_graph_schema.h"
#include "catalog/rel_table_group_schema.h"
#include "catalog/rel_table_schema.h"
#include "common/cast.h"
#include "common/exception/catalog.h"
#include "common/exception/runtime.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/string_format.h"
#include "common/string_utils.h"
#include "storage/storage_utils.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace catalog {

CatalogContent::CatalogContent() : nextTableID{0} {
    registerBuiltInFunctions();
}

CatalogContent::CatalogContent(const std::string& directory) {
    readFromFile(directory, FileVersionType::ORIGINAL);
    registerBuiltInFunctions();
}

static void assignPropertyIDAndTableID(
    std::vector<std::unique_ptr<Property>>& properties, table_id_t tableID) {
    for (auto i = 0u; i < properties.size(); ++i) {
        properties[i]->setPropertyID(i);
        properties[i]->setTableID(tableID);
    }
}

table_id_t CatalogContent::addNodeTableSchema(const BoundCreateTableInfo& info) {
    table_id_t tableID = assignNextTableID();
    auto extraInfo = reinterpret_cast<BoundExtraCreateNodeTableInfo*>(info.extraInfo.get());
    auto properties = Property::copy(extraInfo->properties);
    assignPropertyIDAndTableID(properties, tableID);
    auto nodeTableSchema = std::make_unique<NodeTableSchema>(
        info.tableName, tableID, extraInfo->primaryKeyIdx, std::move(properties));
    tableNameToIDMap.emplace(nodeTableSchema->tableName, tableID);
    tableSchemas.emplace(tableID, std::move(nodeTableSchema));
    return tableID;
}

// TODO(Xiyang): move this to binding stage
static void addRelInternalIDProperty(std::vector<std::unique_ptr<Property>>& properties) {
    auto relInternalIDProperty =
        std::make_unique<Property>(InternalKeyword::ID, LogicalType::INTERNAL_ID());
    properties.insert(properties.begin(), std::move(relInternalIDProperty));
}

table_id_t CatalogContent::addRelTableSchema(const BoundCreateTableInfo& info) {
    table_id_t tableID = assignNextTableID();
    auto extraInfo = reinterpret_cast<BoundExtraCreateRelTableInfo*>(info.extraInfo.get());
    auto properties = Property::copy(extraInfo->properties);
    addRelInternalIDProperty(properties);
    assignPropertyIDAndTableID(properties, tableID);
    auto srcNodeTableSchema =
        ku_dynamic_cast<TableSchema*, NodeTableSchema*>(getTableSchema(extraInfo->srcTableID));
    auto dstNodeTableSchema =
        ku_dynamic_cast<TableSchema*, NodeTableSchema*>(getTableSchema(extraInfo->dstTableID));
    KU_ASSERT(srcNodeTableSchema && dstNodeTableSchema);
    srcNodeTableSchema->addFwdRelTableID(tableID);
    dstNodeTableSchema->addBwdRelTableID(tableID);
    auto relTableSchema =
        std::make_unique<RelTableSchema>(info.tableName, tableID, extraInfo->relMultiplicity,
            std::move(properties), extraInfo->srcTableID, extraInfo->dstTableID);
    tableNameToIDMap.emplace(relTableSchema->tableName, tableID);
    tableSchemas.emplace(tableID, std::move(relTableSchema));
    return tableID;
}

table_id_t CatalogContent::addRelTableGroupSchema(const binder::BoundCreateTableInfo& info) {
    table_id_t relTableGroupID = assignNextTableID();
    auto extraInfo = (BoundExtraCreateRelTableGroupInfo*)info.extraInfo.get();
    std::vector<table_id_t> relTableIDs;
    relTableIDs.reserve(extraInfo->infos.size());
    for (auto& childInfo : extraInfo->infos) {
        relTableIDs.push_back(addRelTableSchema(*childInfo));
    }
    auto relTableGroupName = info.tableName;
    auto relTableGroupSchema = std::make_unique<RelTableGroupSchema>(
        relTableGroupName, relTableGroupID, std::move(relTableIDs));
    tableNameToIDMap.emplace(relTableGroupName, relTableGroupID);
    tableSchemas.emplace(relTableGroupID, std::move(relTableGroupSchema));
    return relTableGroupID;
}

table_id_t CatalogContent::addRdfGraphSchema(const BoundCreateTableInfo& info) {
    table_id_t rdfGraphID = assignNextTableID();
    auto extraInfo = reinterpret_cast<BoundExtraCreateRdfGraphInfo*>(info.extraInfo.get());
    auto resourceInfo = extraInfo->resourceInfo.get();
    auto literalInfo = extraInfo->literalInfo.get();
    auto resourceTripleInfo = extraInfo->resourceTripleInfo.get();
    auto literalTripleInfo = extraInfo->literalTripleInfo.get();
    auto resourceTripleExtraInfo =
        reinterpret_cast<BoundExtraCreateRelTableInfo*>(resourceTripleInfo->extraInfo.get());
    auto literalTripleExtraInfo =
        reinterpret_cast<BoundExtraCreateRelTableInfo*>(literalTripleInfo->extraInfo.get());
    // Resource table
    auto resourceTableID = addNodeTableSchema(*resourceInfo);
    // Literal table
    auto literalTableID = addNodeTableSchema(*literalInfo);
    // Resource triple table
    resourceTripleExtraInfo->srcTableID = resourceTableID;
    resourceTripleExtraInfo->dstTableID = resourceTableID;
    auto resourceTripleTableID = addRelTableSchema(*resourceTripleInfo);
    // Literal triple table
    literalTripleExtraInfo->srcTableID = resourceTableID;
    literalTripleExtraInfo->dstTableID = literalTableID;
    auto literalTripleTableID = addRelTableSchema(*literalTripleInfo);
    // Rdf table schema
    auto rdfGraphName = info.tableName;
    auto rdfGraphSchema = std::make_unique<RdfGraphSchema>(rdfGraphName, rdfGraphID,
        resourceTableID, literalTableID, resourceTripleTableID, literalTripleTableID);
    tableNameToIDMap.emplace(rdfGraphName, rdfGraphID);
    tableSchemas.emplace(rdfGraphID, std::move(rdfGraphSchema));
    return rdfGraphID;
}

std::vector<TableSchema*> CatalogContent::getTableSchemas() const {
    std::vector<TableSchema*> result;
    for (auto&& [_, schema] : tableSchemas) {
        result.push_back(schema.get());
    }
    return result;
}

std::vector<TableSchema*> CatalogContent::getTableSchemas(
    const std::vector<table_id_t>& tableIDs) const {
    std::vector<TableSchema*> result;
    for (auto tableID : tableIDs) {
        KU_ASSERT(tableSchemas.contains(tableID));
        result.push_back(tableSchemas.at(tableID).get());
    }
    return result;
}

void CatalogContent::dropTableSchema(table_id_t tableID) {
    auto tableSchema = getTableSchema(tableID);
    switch (tableSchema->tableType) {
    case common::TableType::REL_GROUP: {
        auto relTableGroupSchema = reinterpret_cast<RelTableGroupSchema*>(tableSchema);
        for (auto& relTableID : relTableGroupSchema->getRelTableIDs()) {
            dropTableSchema(relTableID);
        }
    } break;
    default:
        break;
    }
    tableNameToIDMap.erase(tableSchema->tableName);
    tableSchemas.erase(tableID);
}

void CatalogContent::renameTable(table_id_t tableID, const std::string& newName) {
    auto tableSchema = getTableSchema(tableID);
    tableNameToIDMap.erase(tableSchema->tableName);
    tableNameToIDMap.emplace(newName, tableID);
    tableSchema->updateTableName(newName);
}

void CatalogContent::saveToFile(const std::string& directory, FileVersionType dbFileType) {
    auto catalogPath = StorageUtils::getCatalogFilePath(directory, dbFileType);
    Serializer serializer(
        std::make_unique<BufferedFileWriter>(FileUtils::openFile(catalogPath, O_WRONLY | O_CREAT)));
    writeMagicBytes(serializer);
    serializer.serializeValue(StorageVersionInfo::getStorageVersion());
    serializer.serializeValue(tableSchemas.size());
    for (auto& [tableID, tableSchema] : tableSchemas) {
        serializer.serializeValue(tableID);
        tableSchema->serialize(serializer);
    }
    serializer.serializeValue(nextTableID);
    serializer.serializeUnorderedMap(macros);
}

void CatalogContent::readFromFile(const std::string& directory, FileVersionType dbFileType) {
    auto catalogPath = StorageUtils::getCatalogFilePath(directory, dbFileType);
    Deserializer deserializer(
        std::make_unique<BufferedFileReader>(FileUtils::openFile(catalogPath, O_RDONLY)));
    validateMagicBytes(deserializer);
    storage_version_t savedStorageVersion;
    deserializer.deserializeValue(savedStorageVersion);
    validateStorageVersion(savedStorageVersion);
    uint64_t numTables;
    deserializer.deserializeValue(numTables);
    table_id_t tableID;
    for (auto i = 0u; i < numTables; i++) {
        deserializer.deserializeValue(tableID);
        tableSchemas[tableID] = TableSchema::deserialize(deserializer);
    }
    for (auto& [tableID_, tableSchema] : tableSchemas) {
        tableNameToIDMap[tableSchema->tableName] = tableID_;
    }
    deserializer.deserializeValue(nextTableID);
    deserializer.deserializeUnorderedMap(macros);
}

ExpressionType CatalogContent::getFunctionType(const std::string& name) const {
    auto upperCaseName = StringUtils::getUpper(name);
    if (macros.contains(upperCaseName)) {
        return ExpressionType::MACRO;
    } else if (!builtInFunctions->containsFunction(name)) {
        throw CatalogException(name + " function does not exist.");
    } else {
        // TODO(Ziyi): we should let table function use the same interface to bind.
        auto funcType = builtInFunctions->getFunctionType(upperCaseName);
        switch (funcType) {
        case function::FunctionType::SCALAR:
            return ExpressionType::FUNCTION;
        case function::FunctionType::AGGREGATE:
            return ExpressionType::AGGREGATE_FUNCTION;
        default:
            KU_UNREACHABLE;
        }
    }
}

void CatalogContent::addFunction(std::string name, function::function_set definitions) {
    StringUtils::toUpper(name);
    builtInFunctions->addFunction(std::move(name), std::move(definitions));
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
    return std::make_unique<CatalogContent>(std::move(tableSchemasToCopy), tableNameToIDMap,
        nextTableID, builtInFunctions->copy(), std::move(macrosToCopy));
}

void CatalogContent::validateStorageVersion(storage_version_t savedStorageVersion) {
    auto storageVersion = StorageVersionInfo::getStorageVersion();
    if (savedStorageVersion != storageVersion) {
        // LCOV_EXCL_START
        throw RuntimeException(
            stringFormat("Trying to read a database file with a different version. "
                         "Database file version: {}, Current build storage version: {}",
                savedStorageVersion, storageVersion));
        // LCOV_EXCL_STOP
    }
}

void CatalogContent::validateMagicBytes(Deserializer& deserializer) {
    auto numMagicBytes = strlen(StorageVersionInfo::MAGIC_BYTES);
    uint8_t magicBytes[4];
    for (auto i = 0u; i < numMagicBytes; i++) {
        deserializer.deserializeValue<uint8_t>(magicBytes[i]);
    }
    if (memcmp(magicBytes, StorageVersionInfo::MAGIC_BYTES, numMagicBytes) != 0) {
        throw RuntimeException(
            "This is not a valid Kuzu database directory for the current version of Kuzu.");
    }
}

void CatalogContent::writeMagicBytes(Serializer& serializer) {
    auto numMagicBytes = strlen(StorageVersionInfo::MAGIC_BYTES);
    for (auto i = 0u; i < numMagicBytes; i++) {
        serializer.serializeValue<uint8_t>(StorageVersionInfo::MAGIC_BYTES[i]);
    }
}

void CatalogContent::registerBuiltInFunctions() {
    builtInFunctions = std::make_unique<function::BuiltInFunctions>();
}

bool CatalogContent::containsTable(const std::string& tableName, TableType tableType) const {
    if (!tableNameToIDMap.contains(tableName)) {
        return false;
    }
    auto tableID = getTableID(tableName);
    return tableSchemas.at(tableID)->tableType == tableType;
}

bool CatalogContent::containsTable(common::TableType tableType) const {
    for (auto& [_, schema] : tableSchemas) {
        if (schema->tableType == tableType) {
            return true;
        }
    }
    return false;
}

std::vector<TableSchema*> CatalogContent::getTableSchemas(TableType tableType) const {
    std::vector<TableSchema*> result;
    for (auto& [id, schema] : tableSchemas) {
        if (schema->getTableType() == tableType) {
            result.push_back(schema.get());
        }
    }
    return result;
}

std::vector<table_id_t> CatalogContent::getTableIDs(TableType tableType) const {
    std::vector<table_id_t> tableIDs;
    for (auto& [id, schema] : tableSchemas) {
        if (schema->getTableType() == tableType) {
            tableIDs.push_back(id);
        }
    }
    return tableIDs;
}

} // namespace catalog
} // namespace kuzu
