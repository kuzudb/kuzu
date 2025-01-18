#include "catalog/catalog.h"

#include "binder/ddl/bound_alter_info.h"
#include "binder/ddl/bound_create_sequence_info.h"
#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog_entry/function_catalog_entry.h"
#include "catalog/catalog_entry/index_catalog_entry.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
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
#include "function/function_collection.h"
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
    indexes = std::make_unique<CatalogSet>();
    internalTables = std::make_unique<CatalogSet>(true /* isInternal */);
    internalSequences = std::make_unique<CatalogSet>(true /* isInternal */);
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
        indexes = std::make_unique<CatalogSet>();
        internalTables = std::make_unique<CatalogSet>(true /* isInternal */);
        internalSequences = std::make_unique<CatalogSet>(true /* isInternal */);
        if (!isInMemMode) {
            // TODO(Guodong): Ideally we should be able to remove this line. Revisit here.
            saveToFile(directory, vfs, FileVersionType::ORIGINAL);
        }
    }
    registerBuiltInFunctions();
}

bool Catalog::containsTable(const Transaction* transaction, const std::string& tableName,
    bool useInternal) const {
    bool contains = tables->containsEntry(transaction, tableName);
    if (!contains && useInternal) {
        contains = internalTables->containsEntry(transaction, tableName);
    }
    return contains;
}

TableCatalogEntry* Catalog::getTableCatalogEntry(const Transaction* transaction,
    table_id_t tableID) const {
    auto result = tables->getEntryOfOID(transaction, tableID);
    if (result == nullptr) {
        result = internalTables->getEntryOfOID(transaction, tableID);
    }
    // LCOV_EXCL_START
    if (result == nullptr) {
        throw RuntimeException(
            stringFormat("Cannot find table catalog entry with id {}.", std::to_string(tableID)));
    }
    // LCOV_EXCL_STOP
    return result->ptrCast<TableCatalogEntry>();
}

TableCatalogEntry* Catalog::getTableCatalogEntry(const Transaction* transaction,
    const std::string& tableName, bool useInternal) const {
    CatalogEntry* result = nullptr;
    if (!tables->containsEntry(transaction, tableName)) {
        if (!useInternal) {
            throw CatalogException(stringFormat("{} does not exist in catalog.", tableName));
        } else {
            result = internalTables->getEntry(transaction, tableName);
        }
    } else {
        result = tables->getEntry(transaction, tableName);
    }
    // LCOV_EXCL_STOP
    return result->ptrCast<TableCatalogEntry>();
}

std::vector<NodeTableCatalogEntry*> Catalog::getNodeTableEntries(Transaction* transaction,
    bool useInternal) const {
    return getTableCatalogEntries<NodeTableCatalogEntry>(transaction,
        CatalogEntryType::NODE_TABLE_ENTRY, useInternal);
}

std::vector<RelTableCatalogEntry*> Catalog::getRelTableEntries(Transaction* transaction,
    bool useInternal) const {
    return getTableCatalogEntries<RelTableCatalogEntry>(transaction,
        CatalogEntryType::REL_TABLE_ENTRY, useInternal);
}

std::vector<RelGroupCatalogEntry*> Catalog::getRelTableGroupEntries(
    Transaction* transaction) const {
    return getTableCatalogEntries<RelGroupCatalogEntry>(transaction,
        CatalogEntryType::REL_GROUP_ENTRY);
}

std::vector<TableCatalogEntry*> Catalog::getTableEntries(const Transaction* transaction) const {
    std::vector<TableCatalogEntry*> result;
    for (auto& [_, entry] : tables->getEntries(transaction)) {
        result.push_back(entry->ptrCast<TableCatalogEntry>());
    }
    return result;
}

bool Catalog::tableInRelGroup(Transaction* transaction, table_id_t tableID) const {
    for (const auto& entry : getRelTableGroupEntries(transaction)) {
        if (entry->isParent(tableID)) {
            return true;
        }
    }
    return false;
}

table_id_t Catalog::createTableEntry(Transaction* transaction, const BoundCreateTableInfo& info) {
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
    default:
        KU_UNREACHABLE;
    }
    const auto tableEntry = entry->constPtrCast<TableCatalogEntry>();
    for (auto& definition : tableEntry->getProperties()) {
        if (definition.getType().getLogicalTypeID() == LogicalTypeID::SERIAL) {
            const auto seqName =
                SequenceCatalogEntry::genSerialName(tableEntry->getName(), definition.getName());
            auto seqInfo =
                BoundCreateSequenceInfo(seqName, 0, 1, 0, std::numeric_limits<int64_t>::max(),
                    false, ConflictAction::ON_CONFLICT_THROW, info.isInternal);
            seqInfo.hasParent = true;
            createSequence(transaction, seqInfo);
        }
    }
    if (info.isInternal) {
        return internalTables->createEntry(transaction, std::move(entry));
    } else {
        return tables->createEntry(transaction, std::move(entry));
    }
}

void Catalog::dropTableEntry(Transaction* transaction, const std::string& name) {
    auto tableID = getTableCatalogEntry(transaction, name)->getTableID();
    dropAllIndexes(transaction, tableID);
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
    default: {
        // DO NOTHING.
    }
    }
    for (auto& definition : tableEntry->getProperties()) {
        if (definition.getType().getLogicalTypeID() == LogicalTypeID::SERIAL) {
            auto seqName =
                SequenceCatalogEntry::genSerialName(tableEntry->getName(), definition.getName());
            dropSequence(transaction, seqName);
        }
    }
    if (tables->containsEntry(transaction, tableEntry->getName())) {}
    auto catalogSetToDrop = tables->containsEntry(transaction, tableEntry->getName()) ?
                                tables.get() :
                                internalTables.get();
    catalogSetToDrop->dropEntry(transaction, tableEntry->getName(), tableEntry->getOID());
}

void Catalog::alterTableEntry(Transaction* transaction, const BoundAlterInfo& info) {
    auto& tableEntry =
        tables->getEntry(transaction, info.tableName)->constCast<TableCatalogEntry>();
    switch (info.alterType) {
    case AlterType::DROP_PROPERTY: {
        auto dropPropertyInfo = info.extraInfo->constCast<BoundExtraDropPropertyInfo>();
        for (auto& [name, catalogEntry] : indexes->getEntries(transaction)) {
            auto& indexCatalogEntry = catalogEntry->constCast<IndexCatalogEntry>();
            if (indexCatalogEntry.getTableID() == tableEntry.getTableID()) {
                throw CatalogException{"Cannot drop a property in a table with indexes."};
            }
        }
    } break;
    default:
        break;
    }
    tables->alterEntry(transaction, info);
}

bool Catalog::containsSequence(const Transaction* transaction, const std::string& name) const {
    return sequences->containsEntry(transaction, name);
}

SequenceCatalogEntry* Catalog::getSequenceEntry(const Transaction* transaction,
    const std::string& sequenceName, bool useInternalSeq) const {
    CatalogEntry* entry = nullptr;
    if (!sequences->containsEntry(transaction, sequenceName) && useInternalSeq) {
        entry = internalSequences->getEntry(transaction, sequenceName);
    } else {
        entry = sequences->getEntry(transaction, sequenceName);
    }
    KU_ASSERT(entry);
    return entry->ptrCast<SequenceCatalogEntry>();
}

SequenceCatalogEntry* Catalog::getSequenceEntry(const Transaction* transaction,
    sequence_id_t sequenceID) const {
    auto entry = internalSequences->getEntryOfOID(transaction, sequenceID);
    if (entry == nullptr) {
        entry = sequences->getEntryOfOID(transaction, sequenceID);
    }
    KU_ASSERT(entry);
    return entry->ptrCast<SequenceCatalogEntry>();
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
    auto entry = std::make_unique<SequenceCatalogEntry>(info);
    entry->setHasParent(info.hasParent);
    if (info.isInternal) {
        return internalSequences->createEntry(transaction, std::move(entry));
    } else {
        return sequences->createEntry(transaction, std::move(entry));
    }
}

void Catalog::dropSequence(Transaction* transaction, const std::string& name) {
    const auto entry = getSequenceEntry(transaction, name);
    dropSequence(transaction, entry->getOID());
}

void Catalog::dropSequence(Transaction* transaction, sequence_id_t sequenceID) {
    const auto sequenceEntry = getSequenceEntry(transaction, sequenceID);
    CatalogSet* set = nullptr;
    set = sequences->containsEntry(transaction, sequenceEntry->getName()) ? sequences.get() :
                                                                            internalSequences.get();
    set->dropEntry(transaction, sequenceEntry->getName(), sequenceEntry->getOID());
}

void Catalog::createType(Transaction* transaction, std::string name, LogicalType type) {
    if (types->containsEntry(transaction, name)) {
        return;
    }
    auto entry = std::make_unique<TypeCatalogEntry>(std::move(name), std::move(type));
    types->createEntry(transaction, std::move(entry));
}

LogicalType Catalog::getType(const Transaction* transaction, const std::string& name) const {
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

void Catalog::createIndex(Transaction* transaction,
    std::unique_ptr<IndexCatalogEntry> indexCatalogEntry) {
    indexes->createEntry(transaction, std::move(indexCatalogEntry));
}

IndexCatalogEntry* Catalog::getIndex(const Transaction* transaction, table_id_t tableID,
    const std::string& indexName) const {
    auto internalName = IndexCatalogEntry::getInternalIndexName(tableID, indexName);
    return indexes->getEntry(transaction, internalName)->ptrCast<IndexCatalogEntry>();
}

std::vector<IndexCatalogEntry*> Catalog::getIndexEntries(const Transaction* transaction) const {
    std::vector<IndexCatalogEntry*> result;
    for (auto& [_, entry] : indexes->getEntries(transaction)) {
        result.push_back(entry->ptrCast<IndexCatalogEntry>());
    }
    return result;
}

bool Catalog::containsIndex(const Transaction* transaction, table_id_t tableID,
    const std::string& indexName) const {
    return indexes->containsEntry(transaction,
        IndexCatalogEntry::getInternalIndexName(tableID, indexName));
}

void Catalog::dropAllIndexes(Transaction* transaction, table_id_t tableID) {
    for (auto catalogEntry : indexes->getEntries(transaction)) {
        auto& indexCatalogEntry = catalogEntry.second->constCast<IndexCatalogEntry>();
        if (indexCatalogEntry.getTableID() == tableID) {
            indexes->dropEntry(transaction, catalogEntry.first, catalogEntry.second->getOID());
        }
    }
}

void Catalog::dropIndex(Transaction* transaction, table_id_t tableID,
    const std::string& indexName) const {
    auto uniqueName = IndexCatalogEntry::getInternalIndexName(tableID, indexName);
    const auto entry = indexes->getEntry(transaction, uniqueName);
    indexes->dropEntry(transaction, uniqueName, entry->getOID());
}

bool Catalog::containsFunction(const Transaction* transaction, const std::string& name) {
    return functions->containsEntry(transaction, name);
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
    if (!containsFunction(transaction, name)) {
        throw CatalogException{stringFormat("function {} doesn't exist.", name)};
    }
    auto entry = getFunctionEntry(transaction, name);
    functions->dropEntry(transaction, name, entry->getOID());
}

CatalogEntry* Catalog::getFunctionEntry(const Transaction* transaction,
    const std::string& name) const {
    if (!functions->containsEntry(transaction, name)) {
        throw CatalogException(stringFormat("function {} does not exist.", name));
    }
    return functions->getEntry(transaction, name);
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

// addScalarMacroFunction
void Catalog::addScalarMacroFunction(Transaction* transaction, std::string name,
    std::unique_ptr<function::ScalarMacroFunction> macro) {
    auto entry = std::make_unique<ScalarMacroCatalogEntry>(std::move(name), std::move(macro));
    functions->createEntry(transaction, std::move(entry));
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
    const auto catalogFile = fs->openFile(catalogPath,
        FileFlags::CREATE_IF_NOT_EXISTS | FileFlags::READ_ONLY | FileFlags::WRITE);
    Serializer serializer(std::make_unique<BufferedFileWriter>(*catalogFile));
    writeMagicBytes(serializer);
    serializer.serializeValue(StorageVersionInfo::getStorageVersion());
    tables->serialize(serializer);
    sequences->serialize(serializer);
    functions->serialize(serializer);
    types->serialize(serializer);
    indexes->serialize(serializer);
    internalTables->serialize(serializer);
    internalSequences->serialize(serializer);
}

void Catalog::readFromFile(const std::string& directory, VirtualFileSystem* fs,
    FileVersionType versionType, main::ClientContext* context) {
    KU_ASSERT(!directory.empty());
    const auto catalogPath = StorageUtils::getCatalogFilePath(fs, directory, versionType);
    Deserializer deserializer(std::make_unique<BufferedFileReader>(
        fs->openFile(catalogPath, FileFlags::READ_ONLY, context)));
    validateMagicBytes(deserializer);
    storage_version_t savedStorageVersion = 0;
    deserializer.deserializeValue(savedStorageVersion);
    validateStorageVersion(savedStorageVersion);
    tables = CatalogSet::deserialize(deserializer);
    sequences = CatalogSet::deserialize(deserializer);
    functions = CatalogSet::deserialize(deserializer);
    types = CatalogSet::deserialize(deserializer);
    indexes = CatalogSet::deserialize(deserializer);
    internalTables = CatalogSet::deserialize(deserializer);
    internalSequences = CatalogSet::deserialize(deserializer);
}

void Catalog::registerBuiltInFunctions() {
    auto functionCollection = function::FunctionCollection::getFunctions();
    for (auto i = 0u; functionCollection[i].name != nullptr; ++i) {
        auto& f = functionCollection[i];
        auto functionSet = f.getFunctionSetFunc();
        functions->createEntry(&DUMMY_TRANSACTION,
            std::make_unique<FunctionCatalogEntry>(f.catalogEntryType, f.name,
                std::move(functionSet)));
    }
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
        extraInfo->dstTableID, extraInfo->storageDirection);
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
        relTableIDs.push_back(createTableEntry(transaction, childInfo));
    }
    return std::make_unique<RelGroupCatalogEntry>(tables.get(), info.tableName,
        std::move(relTableIDs));
}

} // namespace catalog
} // namespace kuzu
