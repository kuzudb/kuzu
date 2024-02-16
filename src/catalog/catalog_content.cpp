#include "catalog/catalog_content.h"

#include <fcntl.h>

#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/cast.h"
#include "common/exception/catalog.h"
#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/string_format.h"
#include "common/string_utils.h"
#include "storage/storage_info.h"
#include "storage/storage_utils.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace catalog {

CatalogContent::CatalogContent(common::VirtualFileSystem* vfs) : nextTableID{0}, vfs{vfs} {
    registerBuiltInFunctions();
    tables = std::make_unique<CatalogSet>();
}

CatalogContent::CatalogContent(const std::string& directory, VirtualFileSystem* vfs) : vfs{vfs} {
    readFromFile(directory, FileVersionType::ORIGINAL);
    registerBuiltInFunctions();
}

table_id_t CatalogContent::createNodeTable(const binder::BoundCreateTableInfo& info) {
    table_id_t tableID = assignNextTableID();
    auto extraInfo = ku_dynamic_cast<BoundExtraCreateTableInfo*, BoundExtraCreateNodeTableInfo*>(
        info.extraInfo.get());
    auto nodeTableEntry =
        std::make_unique<NodeTableCatalogEntry>(info.tableName, tableID, extraInfo->primaryKeyIdx);
    for (auto& propertyInfo : extraInfo->propertyInfos) {
        nodeTableEntry->addProperty(propertyInfo.name, propertyInfo.type.copy());
    }
    tableNameToIDMap.emplace(nodeTableEntry->getName(), tableID);
    tables->createEntry(std::move(nodeTableEntry));
    return tableID;
}

table_id_t CatalogContent::createRelTable(const binder::BoundCreateTableInfo& info) {
    table_id_t tableID = assignNextTableID();
    auto extraInfo = ku_dynamic_cast<BoundExtraCreateTableInfo*, BoundExtraCreateRelTableInfo*>(
        info.extraInfo.get());
    auto srcTableEntry = ku_dynamic_cast<CatalogEntry*, NodeTableCatalogEntry*>(
        getTableCatalogEntry(extraInfo->srcTableID));
    auto dstTableEntry = ku_dynamic_cast<CatalogEntry*, NodeTableCatalogEntry*>(
        getTableCatalogEntry(extraInfo->dstTableID));
    srcTableEntry->addFwdRelTableID(tableID);
    dstTableEntry->addBWdRelTableID(tableID);
    auto relTableEntry =
        std::make_unique<RelTableCatalogEntry>(info.tableName, tableID, extraInfo->srcMultiplicity,
            extraInfo->dstMultiplicity, extraInfo->srcTableID, extraInfo->dstTableID);
    for (auto& propertyInfo : extraInfo->propertyInfos) {
        relTableEntry->addProperty(propertyInfo.name, propertyInfo.type.copy());
    }
    tableNameToIDMap.emplace(relTableEntry->getName(), tableID);
    tables->createEntry(std::move(relTableEntry));
    return tableID;
}

table_id_t CatalogContent::createRelGroup(const binder::BoundCreateTableInfo& info) {
    auto relGroupID = assignNextTableID();
    auto extraInfo = (BoundExtraCreateRelTableGroupInfo*)info.extraInfo.get();
    std::vector<table_id_t> relTableIDs;
    relTableIDs.reserve(extraInfo->infos.size());
    for (auto& childInfo : extraInfo->infos) {
        relTableIDs.push_back(createRelTable(childInfo));
    }
    auto relGroupName = info.tableName;
    auto relGroupEntry =
        std::make_unique<RelGroupCatalogEntry>(relGroupName, relGroupID, std::move(relTableIDs));
    tableNameToIDMap.emplace(relGroupName, relGroupID);
    tables->createEntry(std::move(relGroupEntry));
    return relGroupID;
}

table_id_t CatalogContent::createRDFGraph(const binder::BoundCreateTableInfo& info) {
    table_id_t rdfGraphID = assignNextTableID();
    auto extraInfo = ku_dynamic_cast<BoundExtraCreateTableInfo*, BoundExtraCreateRdfGraphInfo*>(
        info.extraInfo.get());
    auto& resourceInfo = extraInfo->resourceInfo;
    auto& literalInfo = extraInfo->literalInfo;
    auto& resourceTripleInfo = extraInfo->resourceTripleInfo;
    auto& literalTripleInfo = extraInfo->literalTripleInfo;
    auto resourceTripleExtraInfo =
        ku_dynamic_cast<BoundExtraCreateTableInfo*, BoundExtraCreateRelTableInfo*>(
            resourceTripleInfo.extraInfo.get());
    auto literalTripleExtraInfo =
        ku_dynamic_cast<BoundExtraCreateTableInfo*, BoundExtraCreateRelTableInfo*>(
            literalTripleInfo.extraInfo.get());
    // Resource table
    auto resourceTableID = createNodeTable(resourceInfo);
    // Literal table
    auto literalTableID = createNodeTable(literalInfo);
    // Resource triple table
    resourceTripleExtraInfo->srcTableID = resourceTableID;
    resourceTripleExtraInfo->dstTableID = resourceTableID;
    auto resourceTripleTableID = createRelTable(resourceTripleInfo);
    // Literal triple table
    literalTripleExtraInfo->srcTableID = resourceTableID;
    literalTripleExtraInfo->dstTableID = literalTableID;
    auto literalTripleTableID = createRelTable(literalTripleInfo);
    // Rdf graph entry
    auto rdfGraphName = info.tableName;
    auto rdfGraphEntry = std::make_unique<RDFGraphCatalogEntry>(rdfGraphName, rdfGraphID,
        resourceTableID, literalTableID, resourceTripleTableID, literalTripleTableID);
    tableNameToIDMap.emplace(rdfGraphName, rdfGraphID);
    tables->createEntry(std::move(rdfGraphEntry));
    return rdfGraphID;
}

void CatalogContent::dropTable(common::table_id_t tableID) {
    auto tableEntry = getTableCatalogEntry(tableID);
    if (tableEntry->getType() == CatalogEntryType::REL_GROUP_ENTRY) {
        auto relGroupEntry = ku_dynamic_cast<CatalogEntry*, RelGroupCatalogEntry*>(tableEntry);
        for (auto& relTableID : relGroupEntry->getRelTableIDs()) {
            dropTable(relTableID);
        }
    }
    tableNameToIDMap.erase(tableEntry->getName());
    tables->removeEntry(tableEntry->getName());
}

void CatalogContent::renameTable(table_id_t tableID, const std::string& newName) {
    auto tableEntry = getTableCatalogEntry(tableID);
    tableNameToIDMap.erase(tableEntry->getName());
    tableNameToIDMap.emplace(newName, tableID);
    tables->renameEntry(tableEntry->getName(), newName);
}

static void validateStorageVersion(storage_version_t savedStorageVersion) {
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

static void validateMagicBytes(Deserializer& deserializer) {
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

static void writeMagicBytes(Serializer& serializer) {
    auto numMagicBytes = strlen(StorageVersionInfo::MAGIC_BYTES);
    for (auto i = 0u; i < numMagicBytes; i++) {
        serializer.serializeValue<uint8_t>(StorageVersionInfo::MAGIC_BYTES[i]);
    }
}

void CatalogContent::saveToFile(const std::string& directory, FileVersionType dbFileType) {
    auto catalogPath = StorageUtils::getCatalogFilePath(vfs, directory, dbFileType);
    Serializer serializer(
        std::make_unique<BufferedFileWriter>(vfs->openFile(catalogPath, O_WRONLY | O_CREAT)));
    writeMagicBytes(serializer);
    serializer.serializeValue(StorageVersionInfo::getStorageVersion());
    tables->serialize(serializer);
    serializer.serializeValue(nextTableID);
    serializer.serializeUnorderedMap(macros);
}

void CatalogContent::readFromFile(const std::string& directory, FileVersionType dbFileType) {
    auto catalogPath = StorageUtils::getCatalogFilePath(vfs, directory, dbFileType);
    Deserializer deserializer(
        std::make_unique<BufferedFileReader>(vfs->openFile(catalogPath, O_RDONLY)));
    validateMagicBytes(deserializer);
    storage_version_t savedStorageVersion;
    deserializer.deserializeValue(savedStorageVersion);
    validateStorageVersion(savedStorageVersion);
    tables = CatalogSet::deserialize(deserializer);
    for (auto& [name, entry] : tables->getEntries()) {
        tableNameToIDMap[entry->getName()] =
            ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(entry.get())->getTableID();
    }
    deserializer.deserializeValue(nextTableID);
    deserializer.deserializeUnorderedMap(macros);
}

ExpressionType CatalogContent::getFunctionType(const std::string& name) const {
    auto normalizedName = StringUtils::getUpper(name);
    if (macros.contains(normalizedName)) {
        return ExpressionType::MACRO;
    }
    auto functionType = builtInFunctions->getFunctionType(name);
    switch (functionType) {
    case function::FunctionType::SCALAR:
        return ExpressionType::FUNCTION;
    case function::FunctionType::AGGREGATE:
        return ExpressionType::AGGREGATE_FUNCTION;
    default:
        KU_UNREACHABLE;
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
    std::unordered_map<std::string, std::unique_ptr<function::ScalarMacroFunction>> macrosToCopy;
    for (auto& macro : macros) {
        macrosToCopy.emplace(macro.first, macro.second->copy());
    }
    return std::make_unique<CatalogContent>(tables->copy(), tableNameToIDMap, nextTableID,
        builtInFunctions->copy(), std::move(macrosToCopy), vfs);
}

void CatalogContent::registerBuiltInFunctions() {
    builtInFunctions = std::make_unique<function::BuiltInFunctions>();
}

bool CatalogContent::containsTable(const std::string& tableName) const {
    return tableNameToIDMap.contains(tableName);
}

bool CatalogContent::containsTable(CatalogEntryType tableType) const {
    for (auto& [_, entry] : tables->getEntries()) {
        if (entry->getType() == tableType) {
            return true;
        }
    }
    return false;
}

std::string CatalogContent::getTableName(table_id_t tableID) const {
    return getTableCatalogEntry(tableID)->getName();
}

CatalogEntry* CatalogContent::getTableCatalogEntry(table_id_t tableID) const {
    for (auto& [name, table] : tables->getEntries()) {
        auto tableEntry = ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(table.get());
        if (tableEntry->getTableID() == tableID) {
            return table.get();
        }
    }
    KU_ASSERT(false);
}

common::table_id_t CatalogContent::getTableID(const std::string& tableName) const {
    if (tables->containsEntry(tableName)) {
        return ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(tables->getEntry(tableName))
            ->getTableID();
    } else {
        throw common::CatalogException{
            common::stringFormat("Table: {} does not exist.", tableName)};
    }
}

std::vector<table_id_t> CatalogContent::getTableIDs(CatalogEntryType catalogType) const {
    std::vector<table_id_t> tableIDs;
    for (auto& [_, entry] : tables->getEntries()) {
        auto tableEntry = ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(entry.get());
        if (tableEntry->getType() == catalogType) {
            tableIDs.push_back(tableEntry->getTableID());
        }
    }
    return tableIDs;
}

} // namespace catalog
} // namespace kuzu
