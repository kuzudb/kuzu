#include "catalog/catalog.h"

#include "binder/ddl/bound_alter_info.h"
#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "catalog/catalog_entry/scalar_macro_catalog_entry.h"
#include "common/exception/catalog.h"
#include "storage/wal/wal.h"
#include "transaction/transaction.h"
#include "transaction/transaction_action.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace catalog {

Catalog::Catalog(VirtualFileSystem* vfs) : isUpdated{false}, wal{nullptr} {
    readOnlyVersion = std::make_unique<CatalogContent>(vfs);
}

Catalog::Catalog(WAL* wal, VirtualFileSystem* vfs) : isUpdated{false}, wal{wal} {
    readOnlyVersion = std::make_unique<CatalogContent>(wal->getDirectory(), vfs);
}

uint64_t Catalog::getTableCount(Transaction* tx) const {
    return getVersion(tx)->getNumTables();
}

bool Catalog::containsNodeTable(Transaction* tx) const {
    return getVersion(tx)->containsTable(CatalogEntryType::NODE_TABLE_ENTRY);
}

bool Catalog::containsRelTable(Transaction* tx) const {
    return getVersion(tx)->containsTable(CatalogEntryType::REL_TABLE_ENTRY);
}

bool Catalog::containsTable(Transaction* tx, const std::string& tableName) const {
    return getVersion(tx)->containsTable(tableName);
}

table_id_t Catalog::getTableID(Transaction* tx, const std::string& tableName) const {
    return getVersion(tx)->getTableID(tableName);
}

std::vector<table_id_t> Catalog::getNodeTableIDs(Transaction* tx) const {
    return getVersion(tx)->getTableIDs(CatalogEntryType::NODE_TABLE_ENTRY);
}

std::vector<table_id_t> Catalog::getRelTableIDs(Transaction* tx) const {
    return getVersion(tx)->getTableIDs(CatalogEntryType::REL_TABLE_ENTRY);
}

std::string Catalog::getTableName(Transaction* tx, table_id_t tableID) const {
    return getVersion(tx)->getTableName(tableID);
}

TableCatalogEntry* Catalog::getTableCatalogEntry(Transaction* tx, table_id_t tableID) const {
    return ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(
        getVersion(tx)->getTableCatalogEntry(tableID));
}

std::vector<NodeTableCatalogEntry*> Catalog::getNodeTableEntries(Transaction* tx) const {
    return getVersion(tx)->getTableCatalogEntries<NodeTableCatalogEntry*>(
        CatalogEntryType::NODE_TABLE_ENTRY);
}

std::vector<RelTableCatalogEntry*> Catalog::getRelTableEntries(Transaction* tx) const {
    return getVersion(tx)->getTableCatalogEntries<RelTableCatalogEntry*>(
        CatalogEntryType::REL_TABLE_ENTRY);
}

std::vector<RelGroupCatalogEntry*> Catalog::getRelTableGroupEntries(Transaction* tx) const {
    return getVersion(tx)->getTableCatalogEntries<RelGroupCatalogEntry*>(
        CatalogEntryType::REL_GROUP_ENTRY);
}

std::vector<RDFGraphCatalogEntry*> Catalog::getRdfGraphEntries(Transaction* tx) const {
    return getVersion(tx)->getTableCatalogEntries<RDFGraphCatalogEntry*>(
        CatalogEntryType::RDF_GRAPH_ENTRY);
}

std::vector<TableCatalogEntry*> Catalog::getTableEntries(Transaction* tx) const {
    std::vector<TableCatalogEntry*> result;
    for (auto& [_, entry] : getVersion(tx)->tables->getEntries()) {
        result.push_back(ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(entry.get()));
    }
    return result;
}

std::vector<TableCatalogEntry*> Catalog::getTableSchemas(Transaction* tx,
    const table_id_vector_t& tableIDs) const {
    std::vector<TableCatalogEntry*> result;
    for (auto tableID : tableIDs) {
        result.push_back(ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(
            getVersion(tx)->getTableCatalogEntry(tableID)));
    }
    return result;
}

void Catalog::prepareCommitOrRollback(TransactionAction action) {
    if (hasUpdates()) {
        wal->logCatalogRecord();
        if (action == TransactionAction::COMMIT) {
            readWriteVersion->saveToFile(wal->getDirectory(), FileVersionType::WAL_VERSION);
        }
    }
}

void Catalog::checkpointInMemory() {
    if (hasUpdates()) {
        readOnlyVersion = std::move(readWriteVersion);
        resetToNotUpdated();
    }
}

table_id_t Catalog::createTableSchema(const BoundCreateTableInfo& info) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    auto tableID = readWriteVersion->createTable(info);
    logCreateTableToWAL(info, tableID);
    return tableID;
}

void Catalog::logCreateTableToWAL(const BoundCreateTableInfo& info, table_id_t tableID) {
    auto tableEntry = readWriteVersion->getTableCatalogEntry(tableID);
    switch (info.type) {
    case TableType::NODE:
    case TableType::REL: {
        wal->logCreateTableRecord(tableID, info.type);
    } break;
    case TableType::REL_GROUP: {
        auto newRelGroupEntry = ku_dynamic_cast<CatalogEntry*, RelGroupCatalogEntry*>(tableEntry);
        for (auto& relTableID : newRelGroupEntry->getRelTableIDs()) {
            wal->logCreateTableRecord(relTableID, TableType::REL);
        }
    } break;
    case TableType::RDF: {
        auto rdfGraphEntry = ku_dynamic_cast<CatalogEntry*, RDFGraphCatalogEntry*>(tableEntry);
        wal->logCreateRdfGraphRecord(tableID, rdfGraphEntry->getResourceTableID(),
            rdfGraphEntry->getLiteralTableID(), rdfGraphEntry->getResourceTripleTableID(),
            rdfGraphEntry->getLiteralTripleTableID());
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void Catalog::dropTableSchema(table_id_t tableID) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    auto tableEntry = readWriteVersion->getTableCatalogEntry(tableID);
    switch (tableEntry->getType()) {
    case CatalogEntryType::REL_GROUP_ENTRY: {
        auto nodeTableEntry = ku_dynamic_cast<CatalogEntry*, RelGroupCatalogEntry*>(tableEntry);
        auto relTableIDs = nodeTableEntry->getRelTableIDs();
        readWriteVersion->dropTable(tableID);
        for (auto relTableID : relTableIDs) {
            wal->logDropTableRecord(relTableID);
        }
    } break;
    case CatalogEntryType::RDF_GRAPH_ENTRY: {
        auto rdfGraphSchema = ku_dynamic_cast<CatalogEntry*, RDFGraphCatalogEntry*>(tableEntry);
        auto graphID = rdfGraphSchema->getTableID();
        auto rtTableID = rdfGraphSchema->getResourceTripleTableID();
        auto ltTableID = rdfGraphSchema->getLiteralTripleTableID();
        auto rTableID = rdfGraphSchema->getResourceTableID();
        auto lTableID = rdfGraphSchema->getLiteralTableID();
        readWriteVersion->dropTable(rtTableID);
        readWriteVersion->dropTable(ltTableID);
        readWriteVersion->dropTable(rTableID);
        readWriteVersion->dropTable(lTableID);
        readWriteVersion->dropTable(graphID);
        wal->logDropTableRecord(rtTableID);
        wal->logDropTableRecord(ltTableID);
        wal->logDropTableRecord(rTableID);
        wal->logDropTableRecord(lTableID);
        wal->logDropTableRecord(graphID);
    } break;
    default: {
        readWriteVersion->dropTable(tableID);
        wal->logDropTableRecord(tableID);
    }
    }
}

void Catalog::alterTableSchema(const BoundAlterInfo& info) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    readWriteVersion->alterTable(info);
    logAlterTableToWAL(info);
}

void Catalog::logAlterTableToWAL(const BoundAlterInfo& info) {
    auto tableSchema = ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(
        readWriteVersion->getTableCatalogEntry(info.tableID));
    switch (info.alterType) {
    case AlterType::ADD_PROPERTY: {
        auto& addPropInfo =
            ku_dynamic_cast<const BoundExtraAlterInfo&, const BoundExtraAddPropertyInfo&>(
                *info.extraInfo);
        wal->logAddPropertyRecord(info.tableID,
            tableSchema->getPropertyID(addPropInfo.propertyName));
    } break;
    case AlterType::DROP_PROPERTY: {
        auto& dropPropInfo =
            ku_dynamic_cast<const BoundExtraAlterInfo&, const BoundExtraDropPropertyInfo&>(
                *info.extraInfo);
        wal->logDropPropertyRecord(info.tableID, dropPropInfo.propertyID);
    } break;
    default: {
        // DO NOTHING.
    }
    }
}

void Catalog::addFunction(std::string name, function::function_set functionSet) {
    initCatalogContentForWriteTrxIfNecessary();
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    readWriteVersion->addFunction(std::move(name), std::move(functionSet));
}

void Catalog::addBuiltInFunction(std::string name, function::function_set functionSet) {
    readOnlyVersion->addFunction(std::move(name), std::move(functionSet));
}

CatalogSet* Catalog::getFunctions(Transaction* tx) const {
    return getVersion(tx)->functions.get();
}

CatalogEntry* Catalog::getFunctionEntry(Transaction* tx, const std::string& name) {
    auto catalogSet = getVersion(tx)->functions.get();
    if (!catalogSet->containsEntry(name)) {
        throw CatalogException(stringFormat("function {} does not exist.", name));
    }
    return catalogSet->getEntry(name);
}

bool Catalog::containsMacro(Transaction* tx, const std::string& macroName) const {
    return getVersion(tx)->containMacro(macroName);
}

void Catalog::addScalarMacroFunction(std::string name,
    std::unique_ptr<function::ScalarMacroFunction> macro) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    auto scalarMacroCatalogEntry =
        std::make_unique<ScalarMacroCatalogEntry>(std::move(name), std::move(macro));
    readWriteVersion->functions->createEntry(std::move(scalarMacroCatalogEntry));
}

std::vector<std::string> Catalog::getMacroNames(Transaction* tx) const {
    std::vector<std::string> macroNames;
    for (auto& [_, function] : getVersion(tx)->functions->getEntries()) {
        if (function->getType() == CatalogEntryType::SCALAR_MACRO_ENTRY) {
            macroNames.push_back(function->getName());
        }
    }
    return macroNames;
}

void Catalog::setTableComment(table_id_t tableID, const std::string& comment) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    auto tableEntry = readWriteVersion->getTableCatalogEntry(tableID);
    ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(tableEntry)->setComment(comment);
}

CatalogContent* Catalog::getVersion(Transaction* tx) const {
    return tx->getType() == TransactionType::READ_ONLY ? readOnlyVersion.get() :
                                                         readWriteVersion.get();
}

} // namespace catalog
} // namespace kuzu
