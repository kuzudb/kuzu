#include "catalog/catalog_content.h"

#include <fcntl.h>

#include "binder/ddl/bound_alter_info.h"
#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog_entry/function_catalog_entry.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "catalog/catalog_entry/scalar_macro_catalog_entry.h"
#include "common/cast.h"
#include "common/exception/catalog.h"
#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/string_format.h"
#include "function/built_in_function_utils.h"
#include "storage/storage_utils.h"
#include "storage/storage_version_info.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace catalog {

CatalogContent::CatalogContent(VirtualFileSystem* vfs) : nextTableID{0}, vfs{vfs} {
    tables = std::make_unique<CatalogSet>();
    functions = std::make_unique<CatalogSet>();
    registerBuiltInFunctions();
}

CatalogContent::CatalogContent(const std::string& directory, VirtualFileSystem* vfs) : vfs{vfs} {
    readFromFile(directory, FileVersionType::ORIGINAL);
    registerBuiltInFunctions();
}

table_id_t CatalogContent::createTable(const BoundCreateTableInfo& info) {
    table_id_t tableID = assignNextTableID();
    std::unique_ptr<CatalogEntry> entry;
    switch (info.type) {
    case TableType::NODE: {
        entry = createNodeTableEntry(tableID, info);
    } break;
    case TableType::REL: {
        entry = createRelTableEntry(tableID, info);
    } break;
    case TableType::REL_GROUP: {
        entry = createRelTableGroupEntry(tableID, info);
    } break;
    case TableType::RDF: {
        entry = createRdfGraphEntry(tableID, info);
    } break;
    default:
        KU_UNREACHABLE;
    }
    tables->createEntry(std::move(entry));
    return tableID;
}

std::unique_ptr<CatalogEntry> CatalogContent::createNodeTableEntry(table_id_t tableID,
    const BoundCreateTableInfo& info) const {
    auto extraInfo =
        ku_dynamic_cast<BoundExtraCreateCatalogEntryInfo*, BoundExtraCreateNodeTableInfo*>(
            info.extraInfo.get());
    auto nodeTableEntry =
        std::make_unique<NodeTableCatalogEntry>(info.tableName, tableID, extraInfo->primaryKeyIdx);
    for (auto& propertyInfo : extraInfo->propertyInfos) {
        nodeTableEntry->addProperty(propertyInfo.name, propertyInfo.type.copy());
    }
    return nodeTableEntry;
}

std::unique_ptr<CatalogEntry> CatalogContent::createRelTableEntry(table_id_t tableID,
    const BoundCreateTableInfo& info) const {
    auto extraInfo =
        ku_dynamic_cast<BoundExtraCreateCatalogEntryInfo*, BoundExtraCreateRelTableInfo*>(
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
    return relTableEntry;
}

std::unique_ptr<CatalogEntry> CatalogContent::createRelTableGroupEntry(table_id_t tableID,
    const BoundCreateTableInfo& info) {
    auto extraInfo =
        ku_dynamic_cast<BoundExtraCreateCatalogEntryInfo*, BoundExtraCreateRelTableGroupInfo*>(
            info.extraInfo.get());
    std::vector<table_id_t> relTableIDs;
    relTableIDs.reserve(extraInfo->infos.size());
    for (auto& childInfo : extraInfo->infos) {
        relTableIDs.push_back(createTable(childInfo));
    }
    return std::make_unique<RelGroupCatalogEntry>(info.tableName, tableID, std::move(relTableIDs));
}

std::unique_ptr<CatalogEntry> CatalogContent::createRdfGraphEntry(table_id_t tableID,
    const BoundCreateTableInfo& info) {
    auto extraInfo =
        ku_dynamic_cast<BoundExtraCreateCatalogEntryInfo*, BoundExtraCreateRdfGraphInfo*>(
            info.extraInfo.get());
    auto& resourceInfo = extraInfo->resourceInfo;
    auto& literalInfo = extraInfo->literalInfo;
    auto& resourceTripleInfo = extraInfo->resourceTripleInfo;
    auto& literalTripleInfo = extraInfo->literalTripleInfo;
    auto resourceTripleExtraInfo =
        ku_dynamic_cast<BoundExtraCreateCatalogEntryInfo*, BoundExtraCreateRelTableInfo*>(
            resourceTripleInfo.extraInfo.get());
    auto literalTripleExtraInfo =
        ku_dynamic_cast<BoundExtraCreateCatalogEntryInfo*, BoundExtraCreateRelTableInfo*>(
            literalTripleInfo.extraInfo.get());
    // Resource table
    auto resourceTableID = createTable(resourceInfo);
    // Literal table
    auto literalTableID = createTable(literalInfo);
    // Resource triple table
    resourceTripleExtraInfo->srcTableID = resourceTableID;
    resourceTripleExtraInfo->dstTableID = resourceTableID;
    auto resourceTripleTableID = createTable(resourceTripleInfo);
    // Literal triple table
    literalTripleExtraInfo->srcTableID = resourceTableID;
    literalTripleExtraInfo->dstTableID = literalTableID;
    auto literalTripleTableID = createTable(literalTripleInfo);
    // Rdf graph entry
    auto rdfGraphName = info.tableName;
    return std::make_unique<RDFGraphCatalogEntry>(rdfGraphName, tableID, resourceTableID,
        literalTableID, resourceTripleTableID, literalTripleTableID);
}

void CatalogContent::dropTable(table_id_t tableID) {
    auto tableEntry = getTableCatalogEntry(tableID);
    if (tableEntry->getType() == CatalogEntryType::REL_GROUP_ENTRY) {
        auto relGroupEntry = ku_dynamic_cast<CatalogEntry*, RelGroupCatalogEntry*>(tableEntry);
        for (auto& relTableID : relGroupEntry->getRelTableIDs()) {
            dropTable(relTableID);
        }
    }
    tables->removeEntry(tableEntry->getName());
}

void CatalogContent::alterTable(const BoundAlterInfo& info) {
    switch (info.alterType) {
    case AlterType::RENAME_TABLE: {
        auto& renameInfo =
            ku_dynamic_cast<const BoundExtraAlterInfo&, const BoundExtraRenameTableInfo&>(
                *info.extraInfo);
        renameTable(info.tableID, renameInfo.newName);
    } break;
    case AlterType::ADD_PROPERTY: {
        auto& addPropInfo =
            ku_dynamic_cast<const BoundExtraAlterInfo&, const BoundExtraAddPropertyInfo&>(
                *info.extraInfo);
        auto tableEntry =
            ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(getTableCatalogEntry(info.tableID));
        tableEntry->addProperty(addPropInfo.propertyName, addPropInfo.dataType.copy());
    } break;
    case AlterType::RENAME_PROPERTY: {
        auto& renamePropInfo =
            ku_dynamic_cast<const BoundExtraAlterInfo&, const BoundExtraRenamePropertyInfo&>(
                *info.extraInfo);
        auto tableEntry =
            ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(getTableCatalogEntry(info.tableID));
        tableEntry->renameProperty(renamePropInfo.propertyID, renamePropInfo.newName);
    } break;
    case AlterType::DROP_PROPERTY: {
        auto& dropPropInfo =
            ku_dynamic_cast<const BoundExtraAlterInfo&, const BoundExtraDropPropertyInfo&>(
                *info.extraInfo);
        auto tableEntry =
            ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(getTableCatalogEntry(info.tableID));
        tableEntry->dropProperty(dropPropInfo.propertyID);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void CatalogContent::renameTable(table_id_t tableID, const std::string& newName) {
    // TODO(Xiyang/Ziyi): Do we allow renaming of rel table groups?
    auto tableEntry = getTableCatalogEntry(tableID);
    if (tableEntry->getType() == CatalogEntryType::RDF_GRAPH_ENTRY) {
        auto rdfGraphEntry = ku_dynamic_cast<CatalogEntry*, RDFGraphCatalogEntry*>(tableEntry);
        renameTable(rdfGraphEntry->getResourceTableID(),
            RDFGraphCatalogEntry::getResourceTableName(newName));
        renameTable(rdfGraphEntry->getLiteralTableID(),
            RDFGraphCatalogEntry::getLiteralTableName(newName));
        renameTable(rdfGraphEntry->getResourceTripleTableID(),
            RDFGraphCatalogEntry::getResourceTripleTableName(newName));
        renameTable(rdfGraphEntry->getLiteralTripleTableID(),
            RDFGraphCatalogEntry::getLiteralTripleTableName(newName));
    }
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
    functions->serialize(serializer);
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
    deserializer.deserializeValue(nextTableID);
    functions = CatalogSet::deserialize(deserializer);
}

void CatalogContent::addFunction(std::string name, function::function_set definitions) {
    if (functions->containsEntry(name)) {
        throw CatalogException{stringFormat("function {} already exists.", name)};
    }
    functions->createEntry(std::make_unique<FunctionCatalogEntry>(
        CatalogEntryType::SCALAR_FUNCTION_ENTRY, std::move(name), std::move(definitions)));
}

function::ScalarMacroFunction* CatalogContent::getScalarMacroFunction(
    const std::string& name) const {
    return ku_dynamic_cast<CatalogEntry*, ScalarMacroCatalogEntry*>(functions->getEntry(name))
        ->getMacroFunction();
}

std::unique_ptr<CatalogContent> CatalogContent::copy() const {
    std::unordered_map<std::string, std::unique_ptr<function::ScalarMacroFunction>> macrosToCopy;
    return std::make_unique<CatalogContent>(tables->copy(), nextTableID, functions->copy(), vfs);
}

void CatalogContent::registerBuiltInFunctions() {
    function::BuiltInFunctionsUtils::createFunctions(functions.get());
}

bool CatalogContent::containsTable(const std::string& tableName) const {
    return tables->containsEntry(tableName);
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
    KU_UNREACHABLE;
}

table_id_t CatalogContent::getTableID(const std::string& tableName) const {
    if (tables->containsEntry(tableName)) {
        return ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(tables->getEntry(tableName))
            ->getTableID();
    } else {
        throw CatalogException{stringFormat("Table: {} does not exist.", tableName)};
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
