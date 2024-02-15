#include "catalog/catalog.h"

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "storage/wal/wal.h"
#include "transaction/transaction.h"
#include "transaction/transaction_action.h"

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

std::vector<common::table_id_t> Catalog::getNodeTableIDs(Transaction* tx) const {
    return getVersion(tx)->getTableIDs(CatalogEntryType::NODE_TABLE_ENTRY);
}

std::vector<common::table_id_t> Catalog::getRelTableIDs(Transaction* tx) const {
    return getVersion(tx)->getTableIDs(CatalogEntryType::REL_TABLE_ENTRY);
}

std::string Catalog::getTableName(Transaction* tx, table_id_t tableID) const {
    return getVersion(tx)->getTableName(tableID);
}

TableCatalogEntry* Catalog::getTableCatalogEntry(
    transaction::Transaction* tx, common::table_id_t tableID) const {
    return ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(
        getVersion(tx)->getTableCatalogEntry(tableID));
}

std::vector<NodeTableCatalogEntry*> Catalog::getNodeTableEntries(Transaction* tx) const {
    return getVersion(tx)->getTableCatalogEntries<NodeTableCatalogEntry*>(
        CatalogEntryType::NODE_TABLE_ENTRY);
}

std::vector<RelTableCatalogEntry*> Catalog::getRelTableEntries(transaction::Transaction* tx) const {
    return getVersion(tx)->getTableCatalogEntries<RelTableCatalogEntry*>(
        CatalogEntryType::REL_TABLE_ENTRY);
}

std::vector<RelGroupCatalogEntry*> Catalog::getRelTableGroupEntries(
    transaction::Transaction* tx) const {
    return getVersion(tx)->getTableCatalogEntries<RelGroupCatalogEntry*>(
        CatalogEntryType::REL_GROUP_ENTRY);
}

std::vector<RDFGraphCatalogEntry*> Catalog::getRdfGraphEntries(transaction::Transaction* tx) const {
    return getVersion(tx)->getTableCatalogEntries<RDFGraphCatalogEntry*>(
        CatalogEntryType::RDF_GRAPH_ENTRY);
}

std::vector<TableCatalogEntry*> Catalog::getTableEntries(transaction::Transaction* tx) const {
    std::vector<TableCatalogEntry*> result;
    for (auto& [_, entry] : getVersion(tx)->tables->getEntries()) {
        result.push_back(ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(entry.get()));
    }
    return result;
}

std::vector<TableCatalogEntry*> Catalog::getTableSchemas(
    Transaction* tx, const table_id_vector_t& tableIDs) const {
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

ExpressionType Catalog::getFunctionType(Transaction* tx, const std::string& name) const {
    return getVersion(tx)->getFunctionType(name);
}

table_id_t Catalog::addNodeTableSchema(const binder::BoundCreateTableInfo& info) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    return readWriteVersion->createNodeTable(info);
}

table_id_t Catalog::addRelTableSchema(const binder::BoundCreateTableInfo& info) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    return readWriteVersion->createRelTable(info);
}

common::table_id_t Catalog::addRelTableGroupSchema(const binder::BoundCreateTableInfo& info) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    auto tableID = readWriteVersion->createRelGroup(info);
    return tableID;
}

table_id_t Catalog::addRdfGraphSchema(const binder::BoundCreateTableInfo& info) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    return readWriteVersion->createRDFGraph(info);
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

void Catalog::renameTable(table_id_t tableID, const std::string& newName) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    auto tableEntry = readWriteVersion->getTableCatalogEntry(tableID);
    switch (tableEntry->getType()) {
    case CatalogEntryType::RDF_GRAPH_ENTRY: {
        auto rdfGraphSchema = ku_dynamic_cast<CatalogEntry*, RDFGraphCatalogEntry*>(tableEntry);
        readWriteVersion->renameTable(rdfGraphSchema->getResourceTableID(),
            RDFGraphCatalogEntry::getResourceTableName(newName));
        readWriteVersion->renameTable(rdfGraphSchema->getLiteralTableID(),
            RDFGraphCatalogEntry::getLiteralTableName(newName));
        readWriteVersion->renameTable(rdfGraphSchema->getResourceTripleTableID(),
            RDFGraphCatalogEntry::getResourceTripleTableName(newName));
        readWriteVersion->renameTable(rdfGraphSchema->getLiteralTripleTableID(),
            RDFGraphCatalogEntry::getLiteralTripleTableName(newName));
        readWriteVersion->renameTable(tableID, newName);
    } break;
    default: {
        readWriteVersion->renameTable(tableID, newName);
    }
    }
}

void Catalog::addNodeProperty(
    table_id_t tableID, const std::string& propertyName, std::unique_ptr<LogicalType> dataType) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    auto tableEntry = readWriteVersion->getTableCatalogEntry(tableID);
    ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(tableEntry)
        ->addProperty(propertyName, std::move(dataType));
}

void Catalog::addRelProperty(
    table_id_t tableID, const std::string& propertyName, std::unique_ptr<LogicalType> dataType) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    auto tableEntry = readWriteVersion->getTableCatalogEntry(tableID);
    ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(tableEntry)
        ->addProperty(propertyName, std::move(dataType));
}

void Catalog::dropProperty(table_id_t tableID, property_id_t propertyID) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    auto tableEntry = readWriteVersion->getTableCatalogEntry(tableID);
    ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(tableEntry)->dropProperty(propertyID);
    wal->logDropPropertyRecord(tableID, propertyID);
}

void Catalog::renameProperty(
    table_id_t tableID, property_id_t propertyID, const std::string& newName) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    auto tableEntry = readWriteVersion->getTableCatalogEntry(tableID);
    ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(tableEntry)
        ->renameProperty(propertyID, newName);
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

bool Catalog::containsMacro(Transaction* tx, const std::string& macroName) const {
    return getVersion(tx)->containMacro(macroName);
}

void Catalog::addScalarMacroFunction(
    std::string name, std::unique_ptr<function::ScalarMacroFunction> macro) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    readWriteVersion->addScalarMacroFunction(std::move(name), std::move(macro));
}

std::vector<std::string> Catalog::getMacroNames(transaction::Transaction* tx) const {
    std::vector<std::string> macroNames;
    for (auto& macro : getVersion(tx)->macros) {
        macroNames.push_back(macro.first);
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
