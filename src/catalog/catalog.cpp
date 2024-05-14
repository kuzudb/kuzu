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
#include "common/cast.h"
#include "common/exception/catalog.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/string_format.h"
#include "function/built_in_function_utils.h"
#include "storage/storage_utils.h"
#include "storage/storage_version_info.h"
#include "storage/wal/wal.h"
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
    registerBuiltInFunctions();
}

Catalog::Catalog(std::string directory, VirtualFileSystem* fs) {
    if (fs->fileOrPathExists(
            StorageUtils::getCatalogFilePath(fs, directory, FileVersionType::ORIGINAL))) {
        readFromFile(directory, fs, FileVersionType::ORIGINAL);
    } else {
        tables = std::make_unique<CatalogSet>();
        sequences = std::make_unique<CatalogSet>();
        functions = std::make_unique<CatalogSet>();
        saveToFile(directory, fs, FileVersionType::ORIGINAL);
    }
    registerBuiltInFunctions();
}

bool Catalog::containsTable(Transaction* transaction, const std::string& tableName) const {
    return tables->containsEntry(transaction, tableName);
}

table_id_t Catalog::getTableID(Transaction* transaction, const std::string& tableName) const {
    auto entry = tables->getEntry(transaction, tableName);
    KU_ASSERT(entry);
    return ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(entry)->getTableID();
}

std::vector<table_id_t> Catalog::getNodeTableIDs(Transaction* transaction) const {
    std::vector<table_id_t> tableIDs;
    iterateCatalogEntries(transaction, [&](CatalogEntry* entry) {
        if (entry->getType() == CatalogEntryType::NODE_TABLE_ENTRY) {
            auto nodeTableEntry = ku_dynamic_cast<CatalogEntry*, NodeTableCatalogEntry*>(entry);
            tableIDs.push_back(nodeTableEntry->getTableID());
        }
    });
    return tableIDs;
}

std::vector<common::table_id_t> Catalog::getRelTableIDs(Transaction* transaction) const {
    std::vector<table_id_t> tableIDs;
    iterateCatalogEntries(transaction, [&](CatalogEntry* entry) {
        if (entry->getType() == CatalogEntryType::REL_TABLE_ENTRY) {
            auto nodeTableEntry = ku_dynamic_cast<CatalogEntry*, RelTableCatalogEntry*>(entry);
            tableIDs.push_back(nodeTableEntry->getTableID());
        }
    });
    return tableIDs;
}

std::string Catalog::getTableName(Transaction* transaction, table_id_t tableID) const {
    return getTableCatalogEntry(transaction, tableID)->getName();
}

TableCatalogEntry* Catalog::getTableCatalogEntry(Transaction* transaction,
    table_id_t tableID) const {
    TableCatalogEntry* result;
    iterateCatalogEntries(transaction, [&](CatalogEntry* entry) {
        if (ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(entry)->getTableID() == tableID) {
            result = ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(entry);
        }
    });
    KU_ASSERT(result);
    return result;
}

std::vector<NodeTableCatalogEntry*> Catalog::getNodeTableEntries(Transaction* transaction) const {
    return getTableCatalogEntries<NodeTableCatalogEntry>(transaction,
        CatalogEntryType::NODE_TABLE_ENTRY);
}

std::vector<RelTableCatalogEntry*> Catalog::getRelTableEntries(
    transaction::Transaction* transaction) const {
    return getTableCatalogEntries<RelTableCatalogEntry>(transaction,
        CatalogEntryType::REL_TABLE_ENTRY);
}

std::vector<RelGroupCatalogEntry*> Catalog::getRelTableGroupEntries(
    transaction::Transaction* transaction) const {
    return getTableCatalogEntries<RelGroupCatalogEntry>(transaction,
        CatalogEntryType::REL_GROUP_ENTRY);
}

std::vector<RDFGraphCatalogEntry*> Catalog::getRdfGraphEntries(Transaction* tx) const {
    return getTableCatalogEntries<RDFGraphCatalogEntry>(tx, CatalogEntryType::RDF_GRAPH_ENTRY);
}

std::vector<TableCatalogEntry*> Catalog::getTableEntries(Transaction* transaction) const {
    std::vector<TableCatalogEntry*> result;
    for (auto& [_, entry] : tables->getEntries(transaction)) {
        result.push_back(ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(entry));
    }
    return result;
}

std::vector<TableCatalogEntry*> Catalog::getTableEntries(Transaction* transaction,
    const table_id_vector_t& tableIDs) const {
    std::vector<TableCatalogEntry*> result;
    for (auto tableID : tableIDs) {
        result.push_back(ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(
            getTableCatalogEntry(transaction, tableID)));
    }
    return result;
}

bool Catalog::tableInRDFGraph(Transaction* tx, table_id_t tableID) const {
    for (auto& entry : getRdfGraphEntries(tx)) {
        auto set = entry->getTableIDSet();
        if (set.contains(tableID)) {
            return true;
        }
    }
    return false;
}

bool Catalog::tableInRelGroup(Transaction* tx, table_id_t tableID) const {
    for (auto& entry : getRelTableGroupEntries(tx)) {
        if (entry->isParent(tableID)) {
            return true;
        }
    }
    return false;
}

table_id_t Catalog::createTableSchema(transaction::Transaction* transaction,
    const BoundCreateTableInfo& info) {
    table_id_t tableID = tables->assignNextOID();
    std::unique_ptr<CatalogEntry> entry;
    switch (info.type) {
    case TableType::NODE: {
        entry = createNodeTableEntry(transaction, tableID, info);
    } break;
    case TableType::REL: {
        entry = createRelTableEntry(transaction, tableID, info);
    } break;
    case TableType::REL_GROUP: {
        entry = createRelTableGroupEntry(transaction, tableID, info);
    } break;
    case TableType::RDF: {
        entry = createRdfGraphEntry(transaction, tableID, info);
    } break;
    default:
        KU_UNREACHABLE;
    }
    tables->createEntry(transaction, std::move(entry));
    return tableID;
}

void Catalog::dropTableSchema(transaction::Transaction* transaction, table_id_t tableID) {
    auto tableEntry = getTableCatalogEntry(transaction, tableID);
    switch (tableEntry->getType()) {
    case CatalogEntryType::REL_GROUP_ENTRY: {
        auto relGroupEntry = ku_dynamic_cast<CatalogEntry*, RelGroupCatalogEntry*>(tableEntry);
        for (auto& relTableID : relGroupEntry->getRelTableIDs()) {
            dropTableSchema(transaction, relTableID);
        }
    } break;
    case CatalogEntryType::RDF_GRAPH_ENTRY: {
        auto rdfGraphSchema = ku_dynamic_cast<CatalogEntry*, RDFGraphCatalogEntry*>(tableEntry);
        dropTableSchema(transaction, rdfGraphSchema->getResourceTableID());
        dropTableSchema(transaction, rdfGraphSchema->getLiteralTableID());
        dropTableSchema(transaction, rdfGraphSchema->getResourceTripleTableID());
        dropTableSchema(transaction, rdfGraphSchema->getLiteralTripleTableID());
    } break;
    default: {
        // DO NOTHING.
    }
    }
    tables->dropEntry(transaction, tableEntry->getName());
}

void Catalog::alterTableSchema(transaction::Transaction* transaction, const BoundAlterInfo& info) {
    auto tableEntry = getTableCatalogEntry(transaction, info.tableID);
    KU_ASSERT(tableEntry);
    if (tableEntry->getType() == CatalogEntryType::RDF_GRAPH_ENTRY) {
        alterRdfChildTableEntries(transaction, tableEntry, info);
    }
    tables->alterEntry(transaction, info);
}

void Catalog::setTableComment(transaction::Transaction* transaction, table_id_t tableID,
    const std::string& comment) const {
    auto tableEntry = getTableCatalogEntry(transaction, tableID);
    ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(tableEntry)->setComment(comment);
}

bool Catalog::containsSequence(Transaction* transaction, const std::string& sequenceName) const {
    return sequences->containsEntry(transaction, sequenceName);
}

sequence_id_t Catalog::getSequenceID(Transaction* transaction,
    const std::string& sequenceName) const {
    auto entry = sequences->getEntry(transaction, sequenceName);
    KU_ASSERT(entry);
    return ku_dynamic_cast<CatalogEntry*, SequenceCatalogEntry*>(entry)->getSequenceID();
}

SequenceCatalogEntry* Catalog::getSequenceCatalogEntry(Transaction* transaction,
    sequence_id_t sequenceID) const {
    SequenceCatalogEntry* result;
    iterateSequenceCatalogEntries(transaction, [&](CatalogEntry* entry) {
        if (ku_dynamic_cast<CatalogEntry*, SequenceCatalogEntry*>(entry)->getSequenceID() ==
            sequenceID) {
            result = ku_dynamic_cast<CatalogEntry*, SequenceCatalogEntry*>(entry);
        }
    });
    KU_ASSERT(result);
    return result;
}

std::vector<SequenceCatalogEntry*> Catalog::getSequenceEntries(Transaction* transaction) const {
    std::vector<SequenceCatalogEntry*> result;
    for (auto& [_, entry] : sequences->getEntries(transaction)) {
        result.push_back(ku_dynamic_cast<CatalogEntry*, SequenceCatalogEntry*>(entry));
    }
    return result;
}

sequence_id_t Catalog::createSequence(transaction::Transaction* transaction,
    const BoundCreateSequenceInfo& info) {
    sequence_id_t sequenceID = sequences->assignNextOID();
    std::unique_ptr<CatalogEntry> entry =
        std::make_unique<SequenceCatalogEntry>(sequences.get(), sequenceID, info);
    sequences->createEntry(transaction, std::move(entry));
    return sequenceID;
}

void Catalog::dropSequence(transaction::Transaction* transaction, sequence_id_t sequenceID) {
    auto sequenceEntry = getSequenceCatalogEntry(transaction, sequenceID);
    sequences->dropEntry(transaction, sequenceEntry->getName());
}

void Catalog::addFunction(transaction::Transaction* transaction, CatalogEntryType entryType,
    std::string name, function::function_set functionSet) {
    if (functions->containsEntry(transaction, name)) {
        throw CatalogException{common::stringFormat("function {} already exists.", name)};
    }
    functions->createEntry(transaction,
        std::make_unique<FunctionCatalogEntry>(entryType, std::move(name), std::move(functionSet)));
}

void Catalog::addBuiltInFunction(CatalogEntryType entryType, std::string name,
    function::function_set functionSet) {
    addFunction(&DUMMY_WRITE_TRANSACTION, entryType, std::move(name), std::move(functionSet));
}

CatalogSet* Catalog::getFunctions(Transaction*) const {
    return functions.get();
}

CatalogEntry* Catalog::getFunctionEntry(Transaction* transaction, const std::string& name) {
    auto catalogSet = functions.get();
    if (!catalogSet->containsEntry(transaction, name)) {
        throw CatalogException(stringFormat("function {} does not exist.", name));
    }
    return catalogSet->getEntry(transaction, name);
}

bool Catalog::containsMacro(Transaction* transaction, const std::string& macroName) const {
    return functions->containsEntry(transaction, macroName);
}

function::ScalarMacroFunction* Catalog::getScalarMacroFunction(Transaction* transaction,
    const std::string& name) const {
    return ku_dynamic_cast<CatalogEntry*, ScalarMacroCatalogEntry*>(
        functions->getEntry(transaction, name))
        ->getMacroFunction();
}

void Catalog::addScalarMacroFunction(transaction::Transaction* transaction, std::string name,
    std::unique_ptr<function::ScalarMacroFunction> macro) {
    auto scalarMacroCatalogEntry =
        std::make_unique<ScalarMacroCatalogEntry>(std::move(name), std::move(macro));
    functions->createEntry(transaction, std::move(scalarMacroCatalogEntry));
}

std::vector<std::string> Catalog::getMacroNames(Transaction* transaction) const {
    std::vector<std::string> macroNames;
    for (auto& [_, function] : functions->getEntries(transaction)) {
        if (function->getType() == CatalogEntryType::SCALAR_MACRO_ENTRY) {
            macroNames.push_back(function->getName());
        }
    }
    return macroNames;
}

void Catalog::prepareCheckpoint(const std::string& databasePath, WAL* wal, VirtualFileSystem* fs) {
    saveToFile(databasePath, fs, common::FileVersionType::WAL_VERSION);
    wal->logCatalogRecord();
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

void Catalog::saveToFile(const std::string& directory, common::VirtualFileSystem* fs,
    common::FileVersionType versionType) {
    auto catalogPath = StorageUtils::getCatalogFilePath(fs, directory, versionType);
    Serializer serializer(
        std::make_unique<BufferedFileWriter>(fs->openFile(catalogPath, O_WRONLY | O_CREAT)));
    writeMagicBytes(serializer);
    serializer.serializeValue(StorageVersionInfo::getStorageVersion());
    tables->serialize(serializer);
    sequences->serialize(serializer);
    functions->serialize(serializer);
}

void Catalog::readFromFile(const std::string& directory, common::VirtualFileSystem* fs,
    common::FileVersionType versionType) {
    auto catalogPath = StorageUtils::getCatalogFilePath(fs, directory, versionType);
    Deserializer deserializer(
        std::make_unique<BufferedFileReader>(fs->openFile(catalogPath, O_RDONLY)));
    validateMagicBytes(deserializer);
    storage_version_t savedStorageVersion;
    deserializer.deserializeValue(savedStorageVersion);
    validateStorageVersion(savedStorageVersion);
    tables = CatalogSet::deserialize(deserializer);
    sequences = CatalogSet::deserialize(deserializer);
    functions = CatalogSet::deserialize(deserializer);
}

void Catalog::registerBuiltInFunctions() {
    function::BuiltInFunctionsUtils::createFunctions(&DUMMY_WRITE_TRANSACTION, functions.get());
}

void Catalog::alterRdfChildTableEntries(transaction::Transaction* transaction,
    kuzu::catalog::CatalogEntry* tableEntry, const binder::BoundAlterInfo& info) const {
    auto rdfGraphEntry = ku_dynamic_cast<CatalogEntry*, RDFGraphCatalogEntry*>(tableEntry);
    auto& renameTableInfo =
        ku_dynamic_cast<const BoundExtraAlterInfo&, const BoundExtraRenameTableInfo&>(
            *info.extraInfo);
    // Resource table.
    auto resourceEntry = getTableCatalogEntry(transaction, rdfGraphEntry->getResourceTableID());
    auto resourceRenameInfo = std::make_unique<BoundAlterInfo>(AlterType::RENAME_TABLE,
        resourceEntry->getName(), resourceEntry->getTableID(),
        std::make_unique<BoundExtraRenameTableInfo>(
            RDFGraphCatalogEntry::getResourceTableName(renameTableInfo.newName)));
    tables->alterEntry(transaction, *resourceRenameInfo);
    // Literal table.
    auto literalEntry = getTableCatalogEntry(transaction, rdfGraphEntry->getLiteralTableID());
    auto literalRenameInfo = std::make_unique<BoundAlterInfo>(AlterType::RENAME_TABLE,
        literalEntry->getName(), literalEntry->getTableID(),
        std::make_unique<BoundExtraRenameTableInfo>(
            RDFGraphCatalogEntry::getLiteralTableName(renameTableInfo.newName)));
    tables->alterEntry(transaction, *literalRenameInfo);
    // Resource triple table.
    auto resourceTripleEntry =
        getTableCatalogEntry(transaction, rdfGraphEntry->getResourceTripleTableID());
    auto resourceTripleRenameInfo = std::make_unique<BoundAlterInfo>(AlterType::RENAME_TABLE,
        resourceTripleEntry->getName(), resourceTripleEntry->getTableID(),
        std::make_unique<BoundExtraRenameTableInfo>(
            RDFGraphCatalogEntry::getResourceTripleTableName(renameTableInfo.newName)));
    tables->alterEntry(transaction, *resourceTripleRenameInfo);
    // Literal triple table.
    auto literalTripleEntry =
        getTableCatalogEntry(transaction, rdfGraphEntry->getLiteralTripleTableID());
    auto literalTripleRenameInfo = std::make_unique<BoundAlterInfo>(AlterType::RENAME_TABLE,
        literalTripleEntry->getName(), literalTripleEntry->getTableID(),
        std::make_unique<BoundExtraRenameTableInfo>(
            RDFGraphCatalogEntry::getLiteralTripleTableName(renameTableInfo.newName)));
    tables->alterEntry(transaction, *literalTripleRenameInfo);
}

std::unique_ptr<CatalogEntry> Catalog::createNodeTableEntry(transaction::Transaction*,
    common::table_id_t tableID, const binder::BoundCreateTableInfo& info) const {
    auto extraInfo =
        ku_dynamic_cast<BoundExtraCreateCatalogEntryInfo*, BoundExtraCreateNodeTableInfo*>(
            info.extraInfo.get());
    auto nodeTableEntry = std::make_unique<NodeTableCatalogEntry>(tables.get(), info.tableName,
        tableID, extraInfo->primaryKeyIdx);
    for (auto& propertyInfo : extraInfo->propertyInfos) {
        nodeTableEntry->addProperty(propertyInfo.name, propertyInfo.type.copy());
    }
    return nodeTableEntry;
}

std::unique_ptr<CatalogEntry> Catalog::createRelTableEntry(transaction::Transaction* transaction,
    common::table_id_t tableID, const binder::BoundCreateTableInfo& info) const {
    auto extraInfo =
        ku_dynamic_cast<BoundExtraCreateCatalogEntryInfo*, BoundExtraCreateRelTableInfo*>(
            info.extraInfo.get());
    auto srcTableEntry = ku_dynamic_cast<CatalogEntry*, NodeTableCatalogEntry*>(
        getTableCatalogEntry(transaction, extraInfo->srcTableID));
    auto dstTableEntry = ku_dynamic_cast<CatalogEntry*, NodeTableCatalogEntry*>(
        getTableCatalogEntry(transaction, extraInfo->dstTableID));
    srcTableEntry->addFwdRelTableID(tableID);
    dstTableEntry->addBWdRelTableID(tableID);
    auto relTableEntry = std::make_unique<RelTableCatalogEntry>(tables.get(), info.tableName,
        tableID, extraInfo->srcMultiplicity, extraInfo->dstMultiplicity, extraInfo->srcTableID,
        extraInfo->dstTableID);
    for (auto& propertyInfo : extraInfo->propertyInfos) {
        relTableEntry->addProperty(propertyInfo.name, propertyInfo.type.copy());
    }
    return relTableEntry;
}

std::unique_ptr<CatalogEntry> Catalog::createRelTableGroupEntry(
    transaction::Transaction* transaction, common::table_id_t tableID,
    const binder::BoundCreateTableInfo& info) {
    auto extraInfo =
        ku_dynamic_cast<BoundExtraCreateCatalogEntryInfo*, BoundExtraCreateRelTableGroupInfo*>(
            info.extraInfo.get());
    std::vector<table_id_t> relTableIDs;
    relTableIDs.reserve(extraInfo->infos.size());
    for (auto& childInfo : extraInfo->infos) {
        relTableIDs.push_back(createTableSchema(transaction, childInfo));
    }
    return std::make_unique<RelGroupCatalogEntry>(tables.get(), info.tableName, tableID,
        std::move(relTableIDs));
}

std::unique_ptr<CatalogEntry> Catalog::createRdfGraphEntry(transaction::Transaction* transaction,
    common::table_id_t tableID, const binder::BoundCreateTableInfo& info) {
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
    return std::make_unique<RDFGraphCatalogEntry>(tables.get(), rdfGraphName, tableID,
        resourceTableID, literalTableID, resourceTripleTableID, literalTripleTableID);
}

} // namespace catalog
} // namespace kuzu
