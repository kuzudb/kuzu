#include "catalog/catalog.h"

#include <fcntl.h>

#include "binder/ddl/bound_alter_info.h"
#include "binder/ddl/bound_create_sequence_info.h"
#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog_entry/function_catalog_entry.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "catalog/catalog_entry/scalar_macro_catalog_entry.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "catalog/catalog_entry/type_catalog_entry.h"
#include "common/exception/catalog.h"
#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/string_format.h"
#include "function/built_in_function_utils.h"
#include "main/db_config.h"
#include "storage/storage_utils.h"
#include "storage/storage_version_info.h"
#include "transaction/transaction.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace catalog {

Catalog::Catalog() {
    tables = std::make_unique<CatalogSet>();
    sequences = std::make_unique<CatalogSet>();
    functions = std::make_unique<CatalogSet>();
    types = std::make_unique<CatalogSet>();
    registerBuiltInFunctions();
}

Catalog::Catalog(const std::string& directory, VirtualFileSystem* vfs) {
    const auto isInMemMode = main::DBConfig::isDBPathInMemory(directory);
    if (!isInMemMode && vfs->fileOrPathExists(StorageUtils::getCatalogFilePath(vfs, directory,
                            FileVersionType::ORIGINAL))) {
        readFromFile(directory, vfs, FileVersionType::ORIGINAL);
    } else {
        tables = std::make_unique<CatalogSet>();
        sequences = std::make_unique<CatalogSet>();
        functions = std::make_unique<CatalogSet>();
        types = std::make_unique<CatalogSet>();
        if (!isInMemMode) {
            // TODO(Guodong): Ideally we should be able to remove this line. Revisit here.
            saveToFile(directory, vfs, FileVersionType::ORIGINAL);
        }
    }
    registerBuiltInFunctions();
}

bool Catalog::containsTable(const Transaction* transaction, const std::string& tableName) const {
    return tables->containsEntry(transaction, tableName);
}

table_id_t Catalog::getTableID(const Transaction* transaction, const std::string& tableName) const {
    return getTableCatalogEntry(transaction, tableName)->getTableID();
}

std::vector<table_id_t> Catalog::getNodeTableIDs(const Transaction* transaction) const {
    std::vector<table_id_t> tableIDs;
    tables->iterateEntriesOfType(transaction, CatalogEntryType::NODE_TABLE_ENTRY,
        [&](const CatalogEntry* entry) { tableIDs.push_back(entry->getOID()); });
    return tableIDs;
}

std::vector<table_id_t> Catalog::getRelTableIDs(const Transaction* transaction) const {
    std::vector<table_id_t> tableIDs;
    tables->iterateEntriesOfType(transaction, CatalogEntryType::REL_TABLE_ENTRY,
        [&](const CatalogEntry* entry) { tableIDs.push_back(entry->getOID()); });
    return tableIDs;
}

std::string Catalog::getTableName(const Transaction* transaction, table_id_t tableID) const {
    return getTableCatalogEntry(transaction, tableID)->getName();
}

TableCatalogEntry* Catalog::getTableCatalogEntry(const Transaction* transaction,
    table_id_t tableID) const {
    const auto result = tables->getEntryOfOID(transaction, tableID);
    // LCOV_EXCL_START
    if (result == nullptr) {
        throw RuntimeException(
            stringFormat("Cannot find table catalog entry with id {}.", std::to_string(tableID)));
    }
    // LCOV_EXCL_STOP
    return result->ptrCast<TableCatalogEntry>();
}

TableCatalogEntry* Catalog::getTableCatalogEntry(const Transaction* transaction,
    const std::string& tableName) const {
    const auto result = tables->getEntry(transaction, tableName);
    if (result == nullptr) {
        throw RuntimeException(
            stringFormat("Cannot find table catalog entry with name {}.", tableName));
    }
    // LCOV_EXCL_STOP
    return result->ptrCast<TableCatalogEntry>();
}

std::vector<NodeTableCatalogEntry*> Catalog::getNodeTableEntries(Transaction* transaction) const {
    return getTableCatalogEntries<NodeTableCatalogEntry>(transaction,
        CatalogEntryType::NODE_TABLE_ENTRY);
}

std::vector<RelTableCatalogEntry*> Catalog::getRelTableEntries(Transaction* transaction) const {
    return getTableCatalogEntries<RelTableCatalogEntry>(transaction,
        CatalogEntryType::REL_TABLE_ENTRY);
}

std::vector<RelGroupCatalogEntry*> Catalog::getRelTableGroupEntries(
    Transaction* transaction) const {
    return getTableCatalogEntries<RelGroupCatalogEntry>(transaction,
        CatalogEntryType::REL_GROUP_ENTRY);
}

std::vector<RDFGraphCatalogEntry*> Catalog::getRdfGraphEntries(Transaction* transaction) const {
    return getTableCatalogEntries<RDFGraphCatalogEntry>(transaction,
        CatalogEntryType::RDF_GRAPH_ENTRY);
}

std::vector<TableCatalogEntry*> Catalog::getTableEntries(const Transaction* transaction) const {
    std::vector<TableCatalogEntry*> result;
    for (auto& [_, entry] : tables->getEntries(transaction)) {
        result.push_back(entry->ptrCast<TableCatalogEntry>());
    }
    return result;
}

std::vector<TableCatalogEntry*> Catalog::getTableEntries(const Transaction* transaction,
    const table_id_vector_t& tableIDs) const {
    std::vector<TableCatalogEntry*> result;
    for (const auto tableID : tableIDs) {
        result.push_back(getTableCatalogEntry(transaction, tableID));
    }
    return result;
}

bool Catalog::tableInRDFGraph(Transaction* transaction, table_id_t tableID) const {
    for (const auto& entry : getRdfGraphEntries(transaction)) {
        auto set = entry->getTableIDSet();
        if (set.contains(tableID)) {
            return true;
        }
    }
    return false;
}

bool Catalog::tableInRelGroup(Transaction* transaction, table_id_t tableID) const {
    for (const auto& entry : getRelTableGroupEntries(transaction)) {
        if (entry->isParent(tableID)) {
            return true;
        }
    }
    return false;
}

table_id_set_t Catalog::getFwdRelTableIDs(Transaction* transaction, table_id_t nodeTableID) const {
    KU_ASSERT(getTableCatalogEntry(transaction, nodeTableID)->getTableType() == TableType::NODE);
    table_id_set_t result;
    for (const auto& relEntry : getRelTableEntries(transaction)) {
        if (relEntry->getSrcTableID() == nodeTableID) {
            result.insert(relEntry->getTableID());
        }
    }
    return result;
}

table_id_set_t Catalog::getBwdRelTableIDs(Transaction* transaction, table_id_t nodeTableID) const {
    KU_ASSERT(getTableCatalogEntry(transaction, nodeTableID)->getTableType() == TableType::NODE);
    table_id_set_t result;
    for (const auto& relEntry : getRelTableEntries(transaction)) {
        if (relEntry->getDstTableID() == nodeTableID) {
            result.insert(relEntry->getTableID());
        }
    }
    return result;
}

table_id_t Catalog::createTableSchema(Transaction* transaction, const BoundCreateTableInfo& info) {
    std::unique_ptr<CatalogEntry> entry;
    switch (info.type) {
    case TableType::NODE: {
        entry = createNodeTableEntry(transaction, info);
    } break;
    case TableType::REL: {
        entry = createRelTableEntry(transaction, info);
    } break;
    case TableType::REL_GROUP: {
        entry = createRelTableGroupEntry(transaction, info);
    } break;
    case TableType::RDF: {
        entry = createRdfGraphEntry(transaction, info);
    } break;
    default:
        KU_UNREACHABLE;
    }
    const auto tableEntry = entry->constPtrCast<TableCatalogEntry>();
    for (auto& definition : tableEntry->getProperties()) {
        if (definition.getType().getLogicalTypeID() == LogicalTypeID::SERIAL) {
            const auto seqName = genSerialName(tableEntry->getName(), definition.getName());
            auto seqInfo = BoundCreateSequenceInfo(seqName, 0, 1, 0,
                std::numeric_limits<int64_t>::max(), false, ConflictAction::ON_CONFLICT_THROW);
            seqInfo.hasParent = true;
            createSequence(transaction, seqInfo);
        }
    }
    return tables->createEntry(transaction, std::move(entry));
}

void Catalog::dropTableEntry(Transaction* transaction, const std::string& name) {
    const auto tableID = getTableID(transaction, name);
    dropTableEntry(transaction, tableID);
}

void Catalog::dropTableEntry(Transaction* transaction, table_id_t tableID) {
    const auto tableEntry = getTableCatalogEntry(transaction, tableID);
    switch (tableEntry->getType()) {
    case CatalogEntryType::REL_GROUP_ENTRY: {
        const auto relGroupEntry = tableEntry->constPtrCast<RelGroupCatalogEntry>();
        for (auto& relTableID : relGroupEntry->getRelTableIDs()) {
            dropTableEntry(transaction, relTableID);
        }
    } break;
    case CatalogEntryType::RDF_GRAPH_ENTRY: {
        const auto rdfGraphSchema = tableEntry->ptrCast<RDFGraphCatalogEntry>();
        dropTableEntry(transaction, rdfGraphSchema->getResourceTableID());
        dropTableEntry(transaction, rdfGraphSchema->getLiteralTableID());
        dropTableEntry(transaction, rdfGraphSchema->getResourceTripleTableID());
        dropTableEntry(transaction, rdfGraphSchema->getLiteralTripleTableID());
    } break;
    default: {
        // DO NOTHING.
    }
    }
    for (auto& definition : tableEntry->getProperties()) {
        if (definition.getType().getLogicalTypeID() == LogicalTypeID::SERIAL) {
            auto seqName = genSerialName(tableEntry->getName(), definition.getName());
            dropSequence(transaction, seqName);
        }
    }
    tables->dropEntry(transaction, tableEntry->getName(), tableEntry->getOID());
}

void Catalog::alterTableEntry(Transaction* transaction, const BoundAlterInfo& info) {
    const auto tableEntry = getTableCatalogEntry(transaction, info.tableName);
    KU_ASSERT(tableEntry);
    if (tableEntry->getType() == CatalogEntryType::RDF_GRAPH_ENTRY &&
        info.alterType != AlterType::COMMENT) {
        alterRdfChildTableEntries(transaction, tableEntry, info);
    }
    tables->alterEntry(transaction, info);
}

bool Catalog::containsSequence(const Transaction* transaction,
    const std::string& sequenceName) const {
    return sequences->containsEntry(transaction, sequenceName);
}

sequence_id_t Catalog::getSequenceID(const Transaction* transaction,
    const std::string& sequenceName) const {
    const auto entry = sequences->getEntry(transaction, sequenceName);
    KU_ASSERT(entry);
    return entry->getOID();
}

SequenceCatalogEntry* Catalog::getSequenceCatalogEntry(const Transaction* transaction,
    sequence_id_t sequenceID) const {
    const auto result =
        sequences->getEntryOfOID(transaction, sequenceID)->ptrCast<SequenceCatalogEntry>();
    KU_ASSERT(result);
    return result;
}

std::vector<SequenceCatalogEntry*> Catalog::getSequenceEntries(
    const Transaction* transaction) const {
    std::vector<SequenceCatalogEntry*> result;
    for (auto& [_, entry] : sequences->getEntries(transaction)) {
        result.push_back(entry->ptrCast<SequenceCatalogEntry>());
    }
    return result;
}

sequence_id_t Catalog::createSequence(Transaction* transaction,
    const BoundCreateSequenceInfo& info) {
    auto entry = std::make_unique<SequenceCatalogEntry>(sequences.get(), info);
    entry->setHasParent(info.hasParent);
    return sequences->createEntry(transaction, std::move(entry));
}

void Catalog::dropSequence(Transaction* transaction, const std::string& name) {
    const auto sequenceID = getSequenceID(transaction, name);
    dropSequence(transaction, sequenceID);
}

void Catalog::dropSequence(Transaction* transaction, sequence_id_t sequenceID) {
    const auto sequenceEntry = getSequenceCatalogEntry(transaction, sequenceID);
    sequences->dropEntry(transaction, sequenceEntry->getName(), sequenceEntry->getOID());
}

std::string Catalog::genSerialName(const std::string& tableName, const std::string& propertyName) {
    return std::string(tableName).append("_").append(propertyName).append("_").append("serial");
}

void Catalog::createType(Transaction* transaction, std::string name, LogicalType type) {
    KU_ASSERT(!types->containsEntry(transaction, name));
    auto entry = std::make_unique<TypeCatalogEntry>(std::move(name), std::move(type));
    types->createEntry(transaction, std::move(entry));
}

LogicalType Catalog::getType(const Transaction* transaction, const std::string& name) const {
    LogicalType type;
    if (LogicalType::tryConvertFromString(name, type)) {
        return type;
    }
    if (!types->containsEntry(transaction, name)) {
        throw CatalogException{
            stringFormat("{} is neither an internal type nor a user defined type.", name)};
    }
    return types->getEntry(transaction, name)
        ->constCast<TypeCatalogEntry>()
        .getLogicalType()
        .copy();
}

bool Catalog::containsType(const Transaction* transaction, const std::string& typeName) const {
    return types->containsEntry(transaction, typeName);
}

void Catalog::addFunction(Transaction* transaction, CatalogEntryType entryType, std::string name,
    function::function_set functionSet) {
    if (functions->containsEntry(transaction, name)) {
        throw CatalogException{stringFormat("function {} already exists.", name)};
    }
    functions->createEntry(transaction,
        std::make_unique<FunctionCatalogEntry>(entryType, std::move(name), std::move(functionSet)));
}

void Catalog::dropFunction(Transaction* transaction, const std::string& name) {
    const auto entry = functions->getEntry(transaction, name);
    if (entry == nullptr) {
        throw CatalogException{stringFormat("function {} doesn't exist.", name)};
    }
    functions->dropEntry(transaction, std::move(name), entry->getOID());
}

void Catalog::addBuiltInFunction(CatalogEntryType entryType, std::string name,
    function::function_set functionSet) {
    addFunction(&DUMMY_TRANSACTION, entryType, std::move(name), std::move(functionSet));
}

CatalogSet* Catalog::getFunctions(Transaction*) const {
    return functions.get();
}

CatalogEntry* Catalog::getFunctionEntry(const Transaction* transaction,
    const std::string& name) const {
    const auto catalogSet = functions.get();
    if (!catalogSet->containsEntry(transaction, name)) {
        throw CatalogException(stringFormat("function {} does not exist.", name));
    }
    return catalogSet->getEntry(transaction, name);
}

std::vector<FunctionCatalogEntry*> Catalog::getFunctionEntries(
    const Transaction* transaction) const {
    std::vector<FunctionCatalogEntry*> result;
    for (auto& [_, entry] : functions->getEntries(transaction)) {
        result.push_back(entry->ptrCast<FunctionCatalogEntry>());
    }
    return result;
}

bool Catalog::containsMacro(const Transaction* transaction, const std::string& macroName) const {
    return functions->containsEntry(transaction, macroName);
}

function::ScalarMacroFunction* Catalog::getScalarMacroFunction(const Transaction* transaction,
    const std::string& name) const {
    return functions->getEntry(transaction, name)
        ->constCast<ScalarMacroCatalogEntry>()
        .getMacroFunction();
}

void Catalog::addScalarMacroFunction(Transaction* transaction, std::string name,
    std::unique_ptr<function::ScalarMacroFunction> macro) {
    auto scalarMacroCatalogEntry =
        std::make_unique<ScalarMacroCatalogEntry>(std::move(name), std::move(macro));
    functions->createEntry(transaction, std::move(scalarMacroCatalogEntry));
}

std::vector<std::string> Catalog::getMacroNames(const Transaction* transaction) const {
    std::vector<std::string> macroNames;
    for (auto& [_, function] : functions->getEntries(transaction)) {
        if (function->getType() == CatalogEntryType::SCALAR_MACRO_ENTRY) {
            macroNames.push_back(function->getName());
        }
    }
    return macroNames;
}

void Catalog::checkpoint(const std::string& databasePath, VirtualFileSystem* fs) const {
    KU_ASSERT(!databasePath.empty());
    saveToFile(databasePath, fs, FileVersionType::WAL_VERSION);
}

static void validateStorageVersion(storage_version_t savedStorageVersion) {
    const auto storageVersion = StorageVersionInfo::getStorageVersion();
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
    const auto numMagicBytes = strlen(StorageVersionInfo::MAGIC_BYTES);
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
    const auto numMagicBytes = strlen(StorageVersionInfo::MAGIC_BYTES);
    for (auto i = 0u; i < numMagicBytes; i++) {
        serializer.serializeValue<uint8_t>(StorageVersionInfo::MAGIC_BYTES[i]);
    }
}

void Catalog::saveToFile(const std::string& directory, VirtualFileSystem* fs,
    FileVersionType versionType) const {
    KU_ASSERT(!directory.empty());
    const auto catalogPath = StorageUtils::getCatalogFilePath(fs, directory, versionType);
    const auto catalogFile = fs->openFile(catalogPath, O_WRONLY | O_CREAT);
    Serializer serializer(std::make_unique<BufferedFileWriter>(*catalogFile));
    writeMagicBytes(serializer);
    serializer.serializeValue(StorageVersionInfo::getStorageVersion());
    tables->serialize(serializer);
    sequences->serialize(serializer);
    functions->serialize(serializer);
    types->serialize(serializer);
}

void Catalog::readFromFile(const std::string& directory, VirtualFileSystem* fs,
    FileVersionType versionType, main::ClientContext* context) {
    KU_ASSERT(!directory.empty());
    const auto catalogPath = StorageUtils::getCatalogFilePath(fs, directory, versionType);
    Deserializer deserializer(
        std::make_unique<BufferedFileReader>(fs->openFile(catalogPath, O_RDONLY, context)));
    validateMagicBytes(deserializer);
    storage_version_t savedStorageVersion;
    deserializer.deserializeValue(savedStorageVersion);
    validateStorageVersion(savedStorageVersion);
    tables = CatalogSet::deserialize(deserializer);
    sequences = CatalogSet::deserialize(deserializer);
    functions = CatalogSet::deserialize(deserializer);
    types = CatalogSet::deserialize(deserializer);
}

void Catalog::registerBuiltInFunctions() {
    function::BuiltInFunctionsUtils::createFunctions(&DUMMY_TRANSACTION, functions.get());
}

void Catalog::alterRdfChildTableEntries(Transaction* transaction, CatalogEntry* tableEntry,
    const BoundAlterInfo& info) const {
    const auto rdfGraphEntry = tableEntry->ptrCast<RDFGraphCatalogEntry>();
    auto& renameTableInfo = info.extraInfo->constCast<BoundExtraRenameTableInfo>();
    // Resource table.
    const auto resourceEntry =
        getTableCatalogEntry(transaction, rdfGraphEntry->getResourceTableID());
    const auto resourceRenameInfo =
        std::make_unique<BoundAlterInfo>(AlterType::RENAME_TABLE, resourceEntry->getName(),
            std::make_unique<BoundExtraRenameTableInfo>(
                RDFGraphCatalogEntry::getResourceTableName(renameTableInfo.newName)));
    tables->alterEntry(transaction, *resourceRenameInfo);
    // Literal table.
    const auto literalEntry = getTableCatalogEntry(transaction, rdfGraphEntry->getLiteralTableID());
    const auto literalRenameInfo =
        std::make_unique<BoundAlterInfo>(AlterType::RENAME_TABLE, literalEntry->getName(),
            std::make_unique<BoundExtraRenameTableInfo>(
                RDFGraphCatalogEntry::getLiteralTableName(renameTableInfo.newName)));
    tables->alterEntry(transaction, *literalRenameInfo);
    // Resource triple table.
    const auto resourceTripleEntry =
        getTableCatalogEntry(transaction, rdfGraphEntry->getResourceTripleTableID());
    const auto resourceTripleRenameInfo =
        std::make_unique<BoundAlterInfo>(AlterType::RENAME_TABLE, resourceTripleEntry->getName(),
            std::make_unique<BoundExtraRenameTableInfo>(
                RDFGraphCatalogEntry::getResourceTripleTableName(renameTableInfo.newName)));
    tables->alterEntry(transaction, *resourceTripleRenameInfo);
    // Literal triple table.
    const auto literalTripleEntry =
        getTableCatalogEntry(transaction, rdfGraphEntry->getLiteralTripleTableID());
    const auto literalTripleRenameInfo =
        std::make_unique<BoundAlterInfo>(AlterType::RENAME_TABLE, literalTripleEntry->getName(),
            std::make_unique<BoundExtraRenameTableInfo>(
                RDFGraphCatalogEntry::getLiteralTripleTableName(renameTableInfo.newName)));
    tables->alterEntry(transaction, *literalTripleRenameInfo);
}

std::unique_ptr<CatalogEntry> Catalog::createNodeTableEntry(Transaction*,
    const BoundCreateTableInfo& info) const {
    const auto extraInfo = info.extraInfo->constPtrCast<BoundExtraCreateNodeTableInfo>();
    auto nodeTableEntry = std::make_unique<NodeTableCatalogEntry>(tables.get(), info.tableName,
        extraInfo->primaryKeyName);
    for (auto& definition : extraInfo->propertyDefinitions) {
        nodeTableEntry->addProperty(definition);
    }
    nodeTableEntry->setHasParent(info.hasParent);
    return nodeTableEntry;
}

std::unique_ptr<CatalogEntry> Catalog::createRelTableEntry(Transaction*,
    const BoundCreateTableInfo& info) const {
    const auto extraInfo = info.extraInfo.get()->constPtrCast<BoundExtraCreateRelTableInfo>();
    auto relTableEntry = std::make_unique<RelTableCatalogEntry>(tables.get(), info.tableName,
        extraInfo->srcMultiplicity, extraInfo->dstMultiplicity, extraInfo->srcTableID,
        extraInfo->dstTableID);
    for (auto& definition : extraInfo->propertyDefinitions) {
        relTableEntry->addProperty(definition);
    }
    relTableEntry->setHasParent(info.hasParent);
    return relTableEntry;
}

std::unique_ptr<CatalogEntry> Catalog::createRelTableGroupEntry(Transaction* transaction,
    const BoundCreateTableInfo& info) {
    const auto extraInfo = info.extraInfo->ptrCast<BoundExtraCreateRelTableGroupInfo>();
    std::vector<table_id_t> relTableIDs;
    relTableIDs.reserve(extraInfo->infos.size());
    for (auto& childInfo : extraInfo->infos) {
        childInfo.hasParent = true;
        relTableIDs.push_back(createTableSchema(transaction, childInfo));
    }
    return std::make_unique<RelGroupCatalogEntry>(tables.get(), info.tableName,
        std::move(relTableIDs));
}

std::unique_ptr<CatalogEntry> Catalog::createRdfGraphEntry(Transaction* transaction,
    const BoundCreateTableInfo& info) {
    const auto extraInfo = info.extraInfo->ptrCast<BoundExtraCreateRdfGraphInfo>();
    auto& resourceInfo = extraInfo->resourceInfo;
    auto& literalInfo = extraInfo->literalInfo;
    auto& resourceTripleInfo = extraInfo->resourceTripleInfo;
    auto& literalTripleInfo = extraInfo->literalTripleInfo;
    resourceInfo.hasParent = true;
    literalInfo.hasParent = true;
    resourceTripleInfo.hasParent = true;
    literalTripleInfo.hasParent = true;
    const auto resourceTripleExtraInfo =
        resourceTripleInfo.extraInfo->ptrCast<BoundExtraCreateRelTableInfo>();
    const auto literalTripleExtraInfo =
        literalTripleInfo.extraInfo->ptrCast<BoundExtraCreateRelTableInfo>();
    // Resource table
    auto resourceTableID = createTableSchema(transaction, resourceInfo);
    // Literal table
    auto literalTableID = createTableSchema(transaction, literalInfo);
    // Resource triple table
    resourceTripleExtraInfo->srcTableID = resourceTableID;
    resourceTripleExtraInfo->dstTableID = resourceTableID;
    auto resourceTripleTableID = createTableSchema(transaction, resourceTripleInfo);
    // Literal triple table
    literalTripleExtraInfo->srcTableID = resourceTableID;
    literalTripleExtraInfo->dstTableID = literalTableID;
    auto literalTripleTableID = createTableSchema(transaction, literalTripleInfo);
    // Rdf graph entry
    auto rdfGraphName = info.tableName;
    return std::make_unique<RDFGraphCatalogEntry>(tables.get(), rdfGraphName, resourceTableID,
        literalTableID, resourceTripleTableID, literalTripleTableID);
}

} // namespace catalog
} // namespace kuzu
